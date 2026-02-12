#!/usr/bin/env python3
import sqlite3
from datetime import datetime

conn = sqlite3.connect('trades.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

print("=" * 80)
print("DIAGNÓSTICO DE TRADES EXPIRANDO")
print("=" * 80)
print(f"Fecha actual: {datetime.now().date()}")
print()

# Todas las opciones abiertas
print("TODAS LAS OPCIONES ABIERTAS:")
print("-" * 80)
cursor.execute("""
    SELECT user, ticker, date, strike, type, price, timestamp
    FROM trades
    WHERE opened = 1 AND type IN ('C', 'P') AND date IS NOT NULL
    ORDER BY date, ticker
""")

trades = cursor.fetchall()
print(f"Total: {len(trades)} trades")
print()

for trade in trades:
    print(f"Usuario: {trade['user']}")
    print(f"  Ticker: {trade['ticker']} {trade['date']} {trade['strike']}{trade['type']}")
    print(f"  Precio: {trade['price']}")
    print(f"  Fecha almacenada: '{trade['date']}'")
    print(f"  Timestamp apertura: {trade['timestamp']}")
    
    # Intentar parsear la fecha
    date_str = trade['date']
    try:
        # Intentar formato MM/DD/YY
        parts = date_str.split('/')
        if len(parts) == 3:
            month, day, year = int(parts[0]), int(parts[1]), int(parts[2])
            if year < 100:
                year = 2000 + year if year < 50 else 1900 + year
            parsed_date = datetime(year, month, day).date()
            today = datetime.now().date()
            
            print(f"  Fecha parseada: {parsed_date}")
            print(f"  ¿Expira hoy o antes? {parsed_date <= today}")
            print(f"  Días hasta expiración: {(parsed_date - today).days}")
    except Exception as e:
        print(f"  ERROR parseando fecha: {e}")
    
    print()

print("=" * 80)
print("TRADES CERRADOS RECIENTEMENTE (últimos 5):")
print("-" * 80)
cursor.execute("""
    SELECT user, ticker, date, strike, type, closing_price, closed_timestamp
    FROM trades
    WHERE opened = 0 AND type IN ('C', 'P')
    ORDER BY closed_timestamp DESC
    LIMIT 5
""")

closed = cursor.fetchall()
for trade in closed:
    print(f"{trade['user']}: {trade['ticker']} {trade['date']} {trade['strike']}{trade['type']}")
    print(f"  Cerrado: {trade['closed_timestamp']} @ {trade['closing_price']}")
    print()

conn.close()
