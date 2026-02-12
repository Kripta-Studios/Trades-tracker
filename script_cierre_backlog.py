# script_cierre_backlog.py
import asyncio
from db_handler import get_open_options_expiring_today, close_trade
from tastytrade import Session
import os
from dotenv import load_dotenv

load_dotenv()
session = Session(provider_secret=os.getenv('TASTYTRADE_CLIENT_SECRET'), refresh_token=os.getenv('TASTYTRADE_REFRESH_TOKEN'))

async def close_backlog():
    trades = get_open_options_expiring_today()
    print(f"Encontradas {len(trades)} opciones para cerrar")
    
    for i, trade in enumerate(trades, 1):
        # Cerrar con precio 0.01 (expiradas worthless)
        result = close_trade(
            trade['user'], 
            trade['ticker'], 
            0.01,  # Precio de cierre para opciones expiradas
            trade['date'], 
            trade['strike'], 
            trade['type']
        )
        if result:
            print(f"{i}/{len(trades)}: Cerrada {trade['ticker']} {trade['date']} {trade['strike']}{trade['type']}")
        await asyncio.sleep(0.1)  # Pequeña pausa entre cierres

asyncio.run(close_backlog())

