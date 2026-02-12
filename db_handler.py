import sqlite3
from datetime import datetime, timedelta, time

def get_db_connection():
    """Create a new database connection."""
    conn = sqlite3.connect('trades.db')
    conn.row_factory = sqlite3.Row  # Allows accessing columns by name
    return conn

def initialize_db():
    """Initialize the database and create the trades table if it doesn't exist."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user TEXT NOT NULL,
            ticker TEXT NOT NULL,
            date TEXT,
            strike TEXT,
            type TEXT,
            price REAL NOT NULL,
            qty INTEGER NOT NULL DEFAULT 1,
            avg_down1 REAL,
            avg_down1_qty INTEGER,
            avg_down2 REAL,
            avg_down2_qty INTEGER,
            trim1 REAL,
            trim2 REAL,
            trim3 REAL,
            trim4 REAL,
            closing_price REAL,
            opened INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            closed_timestamp TEXT
        )
        ''')
        # Add new columns if they don't exist (for migration)
        try:
            cursor.execute('ALTER TABLE trades ADD COLUMN avg_down1 REAL')
        except sqlite3.OperationalError:
            pass
        try:
            cursor.execute('ALTER TABLE trades ADD COLUMN avg_down1_qty INTEGER')
        except sqlite3.OperationalError:
            pass
        try:
            cursor.execute('ALTER TABLE trades ADD COLUMN avg_down2 REAL')
        except sqlite3.OperationalError:
            pass
        try:
            cursor.execute('ALTER TABLE trades ADD COLUMN avg_down2_qty INTEGER')
        except sqlite3.OperationalError:
            pass
        conn.commit()

# Initialize the database when the module is loaded
initialize_db()

def is_trade_open(user, ticker, date=None, strike=None, type_opt=None):
    """Check if a trade is open for the given user and ticker."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            query = '''
            SELECT id FROM trades WHERE user=? AND ticker=? AND opened=1
            '''
            params = [user, ticker]

            if date:
                query += " AND date=?"
                params.append(date)
            if strike:
                query += " AND strike=?"
                params.append(strike)
            if type_opt:
                query += " AND type=?"
                params.append(type_opt)

            cursor.execute(query, params)
            result = cursor.fetchone()
            return result is not None
    except sqlite3.Error as e:
        print(f"Database error in is_trade_open: {e}")
        return False

def open_trade(user, ticker, price, qty=1, date=None, strike=None, type_opt=None):
    """Open a new trade and return the opening price and None for closing price."""
    try:
        if is_trade_open(user, ticker, date, strike, type_opt):
            return None  # Trade already open

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
            INSERT INTO trades (user, ticker, date, strike, type, price, qty, opened, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
            ''', (user, ticker, date, strike, type_opt, price, qty, now))
            conn.commit()
        return (price, None)
    except sqlite3.Error as e:
        print(f"Database error in open_trade: {e}")
        return None

def avg_down_trade(user, ticker, avg_price, avg_qty, date=None, strike=None, type_opt=None):
    """Add an average-down price and quantity to the next available avg_down column."""
    try:
        if not is_trade_open(user, ticker, date, strike, type_opt):
            return None  # No open trade found

        with get_db_connection() as conn:
            cursor = conn.cursor()
            query_select = '''
            SELECT id, price, qty, avg_down1, avg_down1_qty, avg_down2, avg_down2_qty
            FROM trades
            WHERE user=? AND ticker=? AND opened=1
            '''
            params_select = [user, ticker]

            if date:
                query_select += " AND date=?"
                params_select.append(date)
            if strike:
                query_select += " AND strike=?"
                params_select.append(strike)
            if type_opt:
                query_select += " AND type=?"
                params_select.append(type_opt)

            cursor.execute(query_select, params_select)
            result = cursor.fetchone()

            if not result:
                return None  # No open trade found

            trade_id, orig_price, orig_qty, avg_down1, avg_down1_qty, avg_down2, avg_down2_qty = result
            avg_count = sum(1 for avg in [avg_down1, avg_down2] if avg is not None)

            if avg_count >= 2:
                return False  # Maximum avg-downs reached

            # Determine the next avg_down column
            avg_column = f"avg_down{avg_count + 1}"
            avg_qty_column = f"avg_down{avg_count + 1}_qty"
            query_update = f'''
            UPDATE trades SET {avg_column}=?, {avg_qty_column}=? WHERE id=?
            '''
            cursor.execute(query_update, (avg_price, avg_qty, trade_id))
            conn.commit()

            # Calculate new average entry price
            prices = [(orig_price, orig_qty)]
            if avg_down1 is not None:
                prices.append((avg_down1, avg_down1_qty))
            if avg_down2 is not None:
                prices.append((avg_down2, avg_down2_qty))
            prices.append((avg_price, avg_qty))

            total_qty = sum(q for _, q in prices)
            avg_entry_price = sum(p * q for p, q in prices) / total_qty if total_qty > 0 else orig_price

            return (avg_entry_price, avg_count + 1)
    except sqlite3.Error as e:
        print(f"Database error in avg_down_trade: {e}")
        return None

def trim_trade(user, ticker, trim_price, date=None, strike=None, type_opt=None):
    """Add a trim price to the next available trim column and return the opening price and trim count."""
    try:
        if not is_trade_open(user, ticker, date, strike, type_opt):
            return None  # No open trade found

        with get_db_connection() as conn:
            cursor = conn.cursor()
            query_select = '''
            SELECT id, price, trim1, trim2, trim3, trim4 FROM trades
            WHERE user=? AND ticker=? AND opened=1
            '''
            params_select = [user, ticker]

            if date:
                query_select += " AND date=?"
                params_select.append(date)
            if strike:
                query_select += " AND strike=?"
                params_select.append(strike)
            if type_opt:
                query_select += " AND type=?"
                params_select.append(type_opt)

            cursor.execute(query_select, params_select)
            result = cursor.fetchone()

            if not result:
                return None  # No open trade found

            trade_id, opening_price, trim1, trim2, trim3, trim4 = result
            trim_count = sum(1 for trim in [trim1, trim2, trim3, trim4] if trim is not None)

            if trim_count >= 4:
                return False  # Maximum trims reached

            # Determine the next trim column
            trim_column = f"trim{trim_count + 1}"
            query_update = f'''
            UPDATE trades SET {trim_column}=? WHERE id=?
            '''
            cursor.execute(query_update, (trim_price, trade_id))
            conn.commit()

            return (opening_price, trim_count + 1)
    except sqlite3.Error as e:
        print(f"Database error in trim_trade: {e}")
        return None

def close_trade(user, ticker, closing_price, date=None, strike=None, type_opt=None):
    """Close an existing trade and return the opening and closing prices."""
    try:
        if not is_trade_open(user, ticker, date, strike, type_opt):
            return None  # No open trade found

        with get_db_connection() as conn:
            cursor = conn.cursor()
            query_select = '''
            SELECT id, price, qty, avg_down1, avg_down1_qty, avg_down2, avg_down2_qty
            FROM trades WHERE user=? AND ticker=? AND opened=1
            '''
            params_select = [user, ticker]

            if date:
                query_select += " AND date=?"
                params_select.append(date)
            if strike:
                query_select += " AND strike=?"
                params_select.append(strike)
            if type_opt:
                query_select += " AND type=?"
                params_select.append(type_opt)

            cursor.execute(query_select, params_select)
            result = cursor.fetchone()

            if not result:
                return None  # No open trade found

            trade_id, orig_price, orig_qty, avg_down1, avg_down1_qty, avg_down2, avg_down2_qty = result

            # Calculate average entry price
            prices = [(orig_price, orig_qty)]
            if avg_down1 is not None:
                prices.append((avg_down1, avg_down1_qty))
            if avg_down2 is not None:
                prices.append((avg_down2, avg_down2_qty))

            total_qty = sum(q for _, q in prices)
            avg_entry_price = sum(p * q for p, q in prices) / total_qty if total_qty > 0 else orig_price

            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            query = '''
            UPDATE trades SET opened=0, closed_timestamp=?, closing_price=?
            WHERE user=? AND ticker=? AND opened=1
            '''
            params = [now, closing_price, user, ticker]

            if date:
                query += " AND date=?"
                params.append(date)
            if strike:
                query += " AND strike=?"
                params.append(strike)
            if type_opt:
                query += " AND type=?"
                params.append(type_opt)

            cursor.execute(query, params)
            conn.commit()

            return (avg_entry_price, closing_price)
    except sqlite3.Error as e:
        print(f"Database error in close_trade: {e}")
        return None

def get_trade_stats(user, timeframe, status):
    """Fetch trade statistics for a user within a timeframe and status."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            now = datetime.now()

            # Define timeframe filter
            if timeframe == "today":
                start_date = datetime.combine(datetime.now(), time.min).strftime('%Y-%m-%d %H:%M:%S')
            elif timeframe == "weekly":
                start_date = (now - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
            elif timeframe == "monthly":
                start_date = (now - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
            elif timeframe == "yearly":
                start_date = (now - timedelta(days=365)).strftime('%Y-%m-%d %H:%M:%S')
            elif timeframe == "all":
                start_date = '1970-01-01 00:00:00'  # Beginning of time for "all"
            else:
                return None  # Invalid timeframe

            query = '''
            SELECT ticker, date, strike, type, price, qty, avg_down1, avg_down1_qty,
                   avg_down2, avg_down2_qty, trim1, trim2, trim3, trim4,
                   closing_price, opened, timestamp, closed_timestamp
            FROM trades WHERE user=? AND timestamp >= ?
            '''
            params = [user, start_date]

            if status == "open":
                query += " AND opened=1"
            elif status == "closed":
                query += " AND opened=0"
            elif status != "all":
                return None  # Invalid status

            cursor.execute(query, params)
            trades = cursor.fetchall()
            return trades
    except sqlite3.Error as e:
        print(f"Database error in get_trade_stats: {e}")
        return None

def get_open_options_expiring_today():
    """Fetch open options trades expiring on or before today."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            today = datetime.now().date()

            # Obtener todas las opciones abiertas
            query = '''
            SELECT user, ticker, date, strike, type, price, qty
            FROM trades
            WHERE opened=1 AND type IN ('C', 'P') AND date IS NOT NULL
            '''
            cursor.execute(query)
            all_trades = cursor.fetchall()

            # Filtrar manualmente por fecha
            expiring_trades = []
            for trade in all_trades:
                try:
                    date_str = trade['date']
                    parsed = None

                    # Intentar parsear diferentes formatos de fecha
                    for fmt in ["%m/%d/%y", "%m/%d/%Y", "%-m/%-d/%y", "%-m/%-d/%Y"]:
                        try:
                            parsed = datetime.strptime(date_str, fmt)
                            break
                        except ValueError:
                            continue

                    # Si no se pudo parsear con formatos estándar, intentar manualmente
                    if not parsed:
                        try:
                            parts = date_str.split('/')
                            if len(parts) == 3:
                                month, day, year = int(parts[0]), int(parts[1]), int(parts[2])
                                # Agregar siglo si es necesario
                                if year < 100:
                                    year = 2000 + year if year < 50 else 1900 + year
                                parsed = datetime(year, month, day)
                        except:
                            pass

                    if parsed and parsed.date() <= today:
                        expiring_trades.append(trade)

                except Exception as e:
                    print(f"Error parsing date '{trade['date']}' for {trade['ticker']}: {e}")
                    continue

            print(f"Found {len(expiring_trades)} options expiring on or before {today}")
            return expiring_trades

    except sqlite3.Error as e:
        print(f"Database error in get_open_options_expiring_today: {e}")
        return []
