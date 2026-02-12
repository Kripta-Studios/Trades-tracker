"""
trading_hours.py - Validación de horarios de mercado para Wall Street y CME
"""
from datetime import datetime, time
from zoneinfo import ZoneInfo
import holidays

# Festivos de Estados Unidos
US_HOLIDAYS = holidays.US()

def is_us_market_holiday(date=None):
    """Verifica si es un día festivo del mercado estadounidense"""
    if date is None:
        date = datetime.now(ZoneInfo("America/New_York")).date()
    return date in US_HOLIDAYS

def is_weekend(date=None):
    """Verifica si es fin de semana"""
    if date is None:
        date = datetime.now(ZoneInfo("America/New_York")).date()
    return date.weekday() >= 5  # 5 = Saturday, 6 = Sunday

def is_options_market_open():
    """
    Verifica si el mercado de opciones está abierto.
    
    Horarios de trading de opciones en Wall Street:
    - Lunes a Viernes: 9:30 AM - 4:00 PM ET
    - Cerrado los fines de semana y festivos
    
    Returns:
        tuple: (bool, str) - (está_abierto, mensaje_de_error)
    """
    now = datetime.now(ZoneInfo("America/New_York"))
    current_date = now.date()
    current_time = now.time()
    
    # Check for weekend
    if is_weekend(current_date):
        day_name = now.strftime("%A")
        return False, f"❌ Options market is closed on {day_name}s. Opens Monday at 9:30 AM ET."
    
    # Check for holidays
    if is_us_market_holiday(current_date):
        holiday_name = US_HOLIDAYS.get(current_date)
        return False, f"❌ Market is closed for {holiday_name}. Opens next business day at 9:30 AM ET."
    
    # Trading hours: 9:30 AM - 4:00 PM ET
    market_open = time(9, 30)
    market_close = time(16, 0)
    
    if current_time < market_open:
        return False, f"❌ Options market opens at 9:30 AM ET. Current time: {now.strftime('%I:%M %p ET')}"
    
    if current_time >= market_close:
        return False, f"❌ Options market closed at 4:00 PM ET. Current time: {now.strftime('%I:%M %p ET')}"
    
    return True, "✅ Market open"

def is_futures_market_open():
    """
    Verifica si el mercado de futuros de la CME está abierto.
    
    Horarios de trading de futuros en CME (E-mini S&P 500, etc):
    - Domingo: 6:00 PM - Viernes 5:00 PM ET (casi 24/5)
    - Cierre diario: 5:00-6:00 PM ET (1 hora de mantenimiento)
    - Cerrado desde el viernes a las 5:00 PM hasta el domingo a las 6:00 PM
    
    Returns:
        tuple: (bool, str) - (está_abierto, mensaje_de_error)
    """
    now = datetime.now(ZoneInfo("America/New_York"))
    current_time = now.time()
    weekday = now.weekday()  # 0=Monday, 6=Sunday
    
    # Horarios
    daily_close_start = time(17, 0)  # 5:00 PM
    daily_open = time(18, 0)         # 6:00 PM
    
    # Friday after 5:00 PM - closed until Sunday 6:00 PM
    if weekday == 4 and current_time >= daily_close_start:  # Friday after 5 PM
        return False, f"❌ Futures are closed on weekends. Opens Sunday at 6:00 PM ET. Current time: {now.strftime('%a %I:%M %p ET')}"
    
    # Saturday - always closed
    if weekday == 5:  # Saturday
        return False, f"❌ Futures are closed on weekends. Opens Sunday at 6:00 PM ET."
    
    # Sunday before 6:00 PM - closed
    if weekday == 6 and current_time < daily_open:  # Sunday before 6 PM
        return False, f"❌ Futures open Sunday at 6:00 PM ET. Current time: {now.strftime('%I:%M %p ET')}"
    
    # Monday to Thursday - daily maintenance window 5:00-6:00 PM
    if weekday < 4 and daily_close_start <= current_time < daily_open:
        return False, f"❌ Futures are in daily maintenance (5:00-6:00 PM ET). Current time: {now.strftime('%I:%M %p ET')}"
    
    return True, "✅ Futures market open"

def is_stock_market_open():
    """
    Verifica si el mercado de acciones está abierto.
    
    Horarios de trading de acciones en Wall Street:
    - Lunes a Viernes: 9:30 AM - 4:00 PM ET
    - Cerrado los fines de semana y festivos
    
    Returns:
        tuple: (bool, str) - (está_abierto, mensaje_de_error)
    """
    now = datetime.now(ZoneInfo("America/New_York"))
    current_date = now.date()
    current_time = now.time()
    
    # Check for weekend
    if is_weekend(current_date):
        day_name = now.strftime("%A")
        return False, f"❌ Stock market is closed on {day_name}s. Opens Monday at 9:30 AM ET."
    
    # Check for holidays
    if is_us_market_holiday(current_date):
        holiday_name = US_HOLIDAYS.get(current_date)
        return False, f"❌ Market is closed for {holiday_name}. Opens next business day at 9:30 AM ET."
    
    # Trading hours: 9:30 AM - 4:00 PM ET
    market_open = time(9, 30)
    market_close = time(16, 0)
    
    if current_time < market_open:
        return False, f"❌ Stock market opens at 9:30 AM ET. Current time: {now.strftime('%I:%M %p ET')}"
    
    if current_time >= market_close:
        return False, f"❌ Stock market closed at 4:00 PM ET. Current time: {now.strftime('%I:%M %p ET')}"
    
    return True, "✅ Market open"

def validate_trading_hours(ticker, trade_type=None):
    """
    Valida si se puede hacer trading según el tipo de instrumento y hora actual.
    
    Args:
        ticker (str): El ticker del instrumento (ej: 'SPY', '/ES', 'AAPL')
        trade_type (str): Tipo de trade ('C', 'P', 'L', 'S', None)
    
    Returns:
        tuple: (bool, str) - (puede_operar, mensaje)
    """
    # Detectar tipo de instrumento
    is_future = '/' in ticker
    is_option = trade_type in ['C', 'P']
    
    if is_future:
        return is_futures_market_open()
    elif is_option:
        return is_options_market_open()
    else:
        # Acciones
        return is_stock_market_open()

# Testing
if __name__ == "__main__":
    print("Testing trading hours validation...")
    print("\n" + "="*60)
    
    now = datetime.now(ZoneInfo("America/New_York"))
    print(f"Hora actual: {now.strftime('%A, %B %d, %Y - %I:%M %p ET')}")
    
    print("\n" + "-"*60)
    print("OPCIONES:")
    is_open, msg = is_options_market_open()
    print(msg)
    
    print("\n" + "-"*60)
    print("FUTUROS:")
    is_open, msg = is_futures_market_open()
    print(msg)
    
    print("\n" + "-"*60)
    print("ACCIONES:")
    is_open, msg = is_stock_market_open()
    print(msg)
    
    print("\n" + "="*60)
    print("\nTEST DE VALIDACIÓN:")
    test_cases = [
        ("SPY", "C", "Opción de SPY"),
        ("/ES", None, "Futuro /ES"),
        ("AAPL", "L", "Acción AAPL"),
    ]
    
    for ticker, trade_type, description in test_cases:
        can_trade, msg = validate_trading_hours(ticker, trade_type)
        print(f"\n{description} ({ticker}):")
        print(f"  {msg}")
