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

def open_trade(user, ticker, price, date=None, strike=None, type_opt=None):
    """Open a new trade and return the opening price and None for closing price."""
    try:
        if is_trade_open(user, ticker, date, strike, type_opt):
            return None  # Trade already open

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
            INSERT INTO trades (user, ticker, date, strike, type, price, opened, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, 1, ?)
            ''', (user, ticker, date, strike, type_opt, price, now))
            conn.commit()
        return (price, None)
    except sqlite3.Error as e:
        print(f"Database error in open_trade: {e}")
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
            SELECT id, price FROM trades WHERE user=? AND ticker=? AND opened=1
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

            trade_id, opening_price = result

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

        return (opening_price, closing_price)
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
            else:
                return None  # Invalid timeframe

            query = '''
            SELECT ticker, date, strike, type, price, trim1, trim2, trim3, trim4, closing_price, opened, timestamp, closed_timestamp
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
            today = datetime.now().strftime("%m/%d/%y").replace("/0", "/")
            if today[0] == '0':
                today = today[1:]

            query = '''
            SELECT user, ticker, date, strike, type, price
            FROM trades
            WHERE opened=1 AND type IN ('C', 'P') AND date<=?
            '''
            cursor.execute(query, (today,))
            trades = cursor.fetchall()
            print("Trades:", trades)
            return trades
    except sqlite3.Error as e:
        print(f"Database error in get_open_options_expiring_today: {e}")
        return []