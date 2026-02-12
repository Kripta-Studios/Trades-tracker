#!/usr/bin/env python3
"""
Script de prueba para verificar que el filtrado de opciones funciona correctamente
"""

import sys
import sqlite3
from datetime import datetime

def get_db_connection():
    """Create a new database connection."""
    conn = sqlite3.connect('trades.db')
    conn.row_factory = sqlite3.Row
    return conn

def parse_option_date(date_str):
    """Parse option date string in multiple formats"""
    for fmt in ["%m/%d/%y", "%m/%d/%Y", "%-m/%-d/%y", "%-m/%-d/%Y"]:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    try:
        parts = date_str.split('/')
        if len(parts) == 3:
            month, day, year = int(parts[0]), int(parts[1]), int(parts[2])
            if year < 100:
                year = 2000 + year if year < 50 else 1900 + year
            return datetime(year, month, day)
    except:
        pass
    
    raise ValueError(f"Cannot parse date: {date_str}")

def get_open_options_expiring_today_OLD():
    """Versión antigua - INCORRECTA"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            today = datetime.now().strftime("%m/%d/%y").replace("/0", "/")
            if today[0] == '0':
                today = today[1:]

            query = '''
            SELECT user, ticker, date, strike, type, price, qty
            FROM trades
            WHERE opened=1 AND type IN ('C', 'P') AND date==?
            '''
            cursor.execute(query, (today,))
            trades = cursor.fetchall()
            return trades
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []

def get_open_options_expiring_today_NEW():
    """Versión nueva - CORREGIDA"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            today = datetime.now().date()
            
            query = '''
            SELECT user, ticker, date, strike, type, price, qty
            FROM trades
            WHERE opened=1 AND type IN ('C', 'P') AND date IS NOT NULL
            '''
            cursor.execute(query)
            all_trades = cursor.fetchall()
            
            expiring_trades = []
            for trade in all_trades:
                try:
                    date_str = trade['date']
                    parsed = parse_option_date(date_str)
                    
                    if parsed.date() <= today:
                        expiring_trades.append(trade)
                        
                except Exception as e:
                    print(f"Error parsing date '{trade['date']}' for {trade['ticker']}: {e}")
                    continue
            
            return expiring_trades
            
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []

if __name__ == "__main__":
    print("=" * 80)
    print("PRUEBA DE FILTRADO DE OPCIONES QUE EXPIRAN")
    print("=" * 80)
    print(f"\nFecha de hoy: {datetime.now().strftime('%m/%d/%Y')}")
    print(f"Fecha como date object: {datetime.now().date()}\n")
    
    print("-" * 80)
    print("VERSIÓN ANTIGUA (INCORRECTA):")
    print("-" * 80)
    old_trades = get_open_options_expiring_today_OLD()
    print(f"Opciones encontradas: {len(old_trades)}")
    if old_trades:
        print("\nPrimeras 5 opciones:")
        for i, t in enumerate(old_trades[:5]):
            print(f"  {i+1}. {t['user']}: {t['ticker']} {t['date']} {t['strike']}{t['type']}")
    
    print("\n" + "-" * 80)
    print("VERSIÓN NUEVA (CORREGIDA):")
    print("-" * 80)
    new_trades = get_open_options_expiring_today_NEW()
    print(f"Opciones encontradas: {len(new_trades)}")
    
    if new_trades:
        print("\nOpciones que realmente han expirado (<=hoy):")
        for i, t in enumerate(new_trades):
            try:
                parsed_date = parse_option_date(t['date'])
                days_ago = (datetime.now().date() - parsed_date.date()).days
                status = "HOY" if days_ago == 0 else f"hace {days_ago} días"
                print(f"  {i+1}. {t['user']}: {t['ticker']} {t['date']} {t['strike']}{t['type']} ({status})")
            except:
                print(f"  {i+1}. {t['user']}: {t['ticker']} {t['date']} {t['strike']}{t['type']}")
    else:
        print("  No hay opciones expiradas")
    
    print("\n" + "=" * 80)
    print(f"RESUMEN:")
    print(f"  Versión antigua encontró: {len(old_trades)} opciones")
    print(f"  Versión nueva encontró: {len(new_trades)} opciones que realmente expiraron")
    print(f"  Diferencia: {len(old_trades) - len(new_trades)} opciones incorrectamente incluidas")
    print("=" * 80)

