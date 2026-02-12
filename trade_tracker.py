#! /usr/bin/python3
import discord
from discord.ext import commands, tasks
from discord.ext.commands import CommandNotFound
import datetime
from zoneinfo import ZoneInfo
from tastytrade import Session
from tasty_handler import tasty_data
from utils import get_future_ticker
from db_handler import open_trade, close_trade, trim_trade, avg_down_trade, get_trade_stats, get_open_options_expiring_today, is_trade_open
from dotenv import load_dotenv
import os
import math
import asyncio
# Imports nuevos
from trading_hours import validate_trading_hours
from stats_calculator import TradeStats

load_dotenv()
DISCORD_TOKEN = os.getenv("DISCORD_BOT_TOKEN_2")
TASTYTRADE_USERNAME = os.getenv("TASTYTRADE_USERNAME")
TASTYTRADE_PASSWORD = os.getenv("TASTYTRADE_PASSWORD")
DISCORD_CHANNEL_ID = os.getenv("DISCORD_CHANNEL_ID")  # Optional: Channel ID for notifications

session = Session(provider_secret=os.getenv('TASTYTRADE_CLIENT_SECRET'), refresh_token=os.getenv('TASTYTRADE_REFRESH_TOKEN'))

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='', intents=intents, case_insensitive=True)

def parse_option_date(date_str):
    """Parse option date string in multiple formats"""
    # Intentar formatos estándar
    for fmt in ["%m/%d/%y", "%m/%d/%Y", "%-m/%-d/%y", "%-m/%-d/%Y"]:
        try:
            return datetime.datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    
    # Si ningún formato funciona, intentar parsing manual
    try:
        parts = date_str.split('/')
        if len(parts) == 3:
            month, day, year = int(parts[0]), int(parts[1]), int(parts[2])
            # Agregar siglo si es necesario
            if year < 100:
                year = 2000 + year if year < 50 else 1900 + year
            return datetime.date(year, month, day)
    except:
        pass
    
    raise ValueError(f"Cannot parse date: {date_str}")

def build_embed(ctx, symbol, price, market, direction_label, extra="", is_long=True, avg_entry_price=None, closing_price=None, trim='', avg=''):
    username = ctx.author.name if ctx else "System"
    color = discord.Color.green() if is_long else discord.Color.red()
    description = f"**{symbol}** {extra} @ **{price}** {trim} {avg} _(Market: {market})_"
    
    if avg_entry_price is not None:
        description += f"\nAvg Entry: {avg_entry_price:.2f}"
    
    if closing_price is not None:
        description += f" | Exit: {closing_price:.2f}"
        if avg_entry_price is not None:
            try:
                if is_long:  # Long trade (BTO/STC)
                    if '/' in symbol:
                        change = closing_price - avg_entry_price
                        sign = "+" if change >= 0 else "-"
                        pct = f"{sign}{abs(change):.2f}pts"
                    else:
                        change = ((closing_price - avg_entry_price) / avg_entry_price) * 100
                        sign = "+" if change >= 0 else "-"
                        pct = f"{sign}{abs(change):.2f}%"
                else:  # Short trade (STO/BTC)
                    if '/' in symbol:
                        change = avg_entry_price - closing_price
                        sign = "+" if change >= 0 else "-"
                        pct = f"{sign}{abs(change):.2f}pts"
                    else:
                        change = ((avg_entry_price - closing_price) / avg_entry_price) * 100
                        sign = "+" if change >= 0 else "-"
                        pct = f"{sign}{abs(change):.2f}%"
                color = discord.Color.green() if sign == "+" else discord.Color.red()
                description += f" | PnL: **{pct}**"
            except ZeroDivisionError:
                description += " | PnL: N/A"

    embed = discord.Embed(
        title=f"{ctx.invoked_with.upper() if ctx else 'STC'} {trim}{avg} Order by {username} {direction_label}",
        description=description,
        color=color
    )

    now_est = datetime.datetime.now(ZoneInfo("America/New_York"))
    formatted_time = now_est.strftime("%Y-%m-%d %I:%M %p EST")
    footer_text = f"\n{formatted_time}\nTrade Tracker Bot by jinskukripta"

    embed.set_footer(text=footer_text)
    return embed

def get_order_direction(command):
    cmd = command.upper()
    if cmd == "BTO":
        return "(long)", True
    elif cmd == "STO":
        return "(short)", False
    elif cmd == "BTC":
        return "(close short)", False
    else:
        return "(close long)", True

@bot.command(name="BTO", aliases=["STO", "STC", "BTC"])
async def order_command(ctx, ticker: str, *args):
    try:
        symbol = ticker.upper()
        
        # ========== MARKET HOURS VALIDATION ==========
        # Determine trade type for validation
        trade_type = None
        
        # If enough arguments, try to detect if it's an option
        if len(args) >= 3:
            for arg in args:
                if str(arg).upper() in ['C', 'P']:
                    trade_type = str(arg).upper()
                    break
        
        # Validate that market is open for this instrument type
        can_trade, market_msg = validate_trading_hours(symbol, trade_type)
        if not can_trade:
            embed = discord.Embed(
                title="Market Closed",
                description=market_msg,
                color=discord.Color.orange()
            )
            now_est = datetime.datetime.now(ZoneInfo("America/New_York"))
            embed.set_footer(text=f"{now_est.strftime('%Y-%m-%d %I:%M %p EST')}")
            await ctx.send(embed=embed)
            return
        # ========== END MARKET HOURS VALIDATION ==========

        # Check for 'trim' or 'avg' and adjust args
        is_trim = len(args) > 0 and args[-1].lower() == "trim"
        is_avg = len(args) > 1 and args[-2].lower() == "avg"
        avg_qty = None
        if is_avg:
            try:
                avg_qty = int(args[-1])
                if avg_qty <= 0:
                    raise ValueError
                args = args[:-2]  # Remove 'avg' and quantity
            except (ValueError, IndexError):
                await ctx.send("Invalid quantity for AVG. Use a positive integer, e.g., 'AVG 230'.")
                return
        elif is_trim:
            args = args[:-1]  # Remove 'trim'

        if len(args) >= 1:
            for i, arg in enumerate(args):
                if arg.startswith('@') and len(arg) > 1:
                    price_str = arg[1:]
                    args = args[:i] + ('@', price_str) + args[i+1:]
                    break
        
        if len(args) == 2:
            at_symbol, price = args
            if at_symbol != "@":
                await ctx.send("Missing '@' before price.")
                return

            try:
                precio_num = float(price)
                if precio_num <= 0:
                    price = "m"
            except:
                price = "m"
            
            if '/' in symbol:
                symbol_tastytrade = get_future_ticker(symbol)
            else:
                symbol_tastytrade = symbol

            _, spot_prices = await tasty_data(session, equities_ticker=[symbol_tastytrade])
            match = next((item for item in spot_prices if item["symbol"] == symbol_tastytrade), None)
            if not match:
                await ctx.send("Ticker not found.")
                return

            market = "{:.2f}".format(float(match.get("mid")))
            if price.lower() == "m":
                price = match.get("last")
                if price == "None":
                    price = market

            if '/' in symbol:
                if float(price) < math.floor(float(match.get("mid")) * 0.9995) or float(price) > math.ceil(float(match.get("mid")) * 1.0005):
                    await ctx.send(f"Your price {price} for {symbol} is too far from current market {market}, use @ m or the current price")
                    return
            if float(price) < float(match.get("mid")) * 0.998 or float(price) > float(match.get("mid")) * 1.002:
                await ctx.send(f"Your price {price} for {symbol} is too far from current market {market}, use @ m or the current price")
                return

            direction_label, is_long = get_order_direction(ctx.invoked_with)
            if ctx.invoked_with.upper() in ["BTO", "STO"]:
                type = "L" if ctx.invoked_with.upper() == "BTO" else "S"
                user_name = ctx.author.name
                if is_avg:
                    if not is_trade_open(user_name, symbol, None, None, type):
                        await ctx.send(f"Trade {symbol} must be open to average down.")
                        return
                    result = avg_down_trade(user_name, symbol, float(price), avg_qty, None, None, type)
                    if result is None:
                        await ctx.send(f"Trade {symbol} is not open")
                        return
                    if result is False:
                        await ctx.send(f"Cannot average down {symbol} more than 2 times.")
                        return
                    avg_entry_price, _ = result
                    closing_price = None  # No closing price for AVG
                else:
                    result = open_trade(user_name, symbol, float(price), 1, None, None, type)
                    if result is None:
                        await ctx.send(f"Trade {symbol} is already opened")
                        return
                    avg_entry_price, closing_price = result
            elif ctx.invoked_with.upper() in ["STC", "BTC"]:
                type = "S" if ctx.invoked_with.upper() == "BTC" else "L"
                user_name = ctx.author.name
                if is_avg:
                    await ctx.send("AVG is not allowed for STC or BTC commands.")
                    return
                if is_trim:
                    result = trim_trade(user_name, symbol, float(price), None, None, type)
                    if result is None:
                        await ctx.send(f"Trade {symbol} is not open")
                        return
                    if result is False:
                        await ctx.send(f"Cannot trim {symbol} more than 4 times. Please close the trade.")
                        return
                    avg_entry_price, _ = result
                    closing_price = float(price)  # Use trim price as closing price for display
                else:
                    result = close_trade(user_name, symbol, float(price), None, None, type)
                    if result is None:
                        await ctx.send(f"Trade {symbol} is not open")
                        return
                    avg_entry_price, closing_price = result
            await ctx.send(f"{ctx.invoked_with.upper()} {symbol} @ {price} {'trim' if is_trim else ''}{'AVG ' + str(avg_qty) if is_avg else ''}")
            embed = build_embed(
                ctx,
                symbol,
                price,
                market,
                direction_label,
                extra="",
                is_long=is_long,
                avg_entry_price=avg_entry_price,
                closing_price=closing_price,
                trim=f"{'trim' if is_trim else ''}",
                avg=f"{'AVG ' + str(avg_qty) if is_avg else ''}"
            )
            await ctx.send(embed=embed)
            return

        elif len(args) == 4:
            date_str, strike_with_type, at_symbol, price = args
            if at_symbol != "@":
                await ctx.send("Missing '@' before price.")
                return

            try:
                exp_date = datetime.datetime.strptime(date_str, "%m/%d/%y").date()
                formatted_date = f"{exp_date.month}/{exp_date.day}/{exp_date.year % 100}"
            except ValueError:
                await ctx.send("Invalid date format. Use MM/DD/YY.")
                return

            try:
                strike = float(strike_with_type[:-1])
                type_option = strike_with_type[-1].upper()
            except:
                await ctx.send("Invalid strike format. Use e.g. 6300P")
                return

            try:
                precio_num = float(price)
                if precio_num <= 0:
                    price = "m"
            except:
                price = "m"

            options_request = {
                "tickers": [symbol],
                "start_date": exp_date,
                "end_date": exp_date,
                "lower_strike": str(strike),
                "upper_strike": str(strike + 1)
            }

            data, _ = await tasty_data(session, options_requested=options_request)

            def is_type_option(symbol: str, type_option: str) -> bool:
                i = len(symbol) - 1
                while i >= 0 and (symbol[i].isdigit() or symbol[i] == "."):
                    i -= 1
                suffix = symbol[i:].lower()
                return type_option.lower() in suffix
            
            match = next((item for item in data if item["strike"] == options_request["lower_strike"] and is_type_option(item["symbol"], type_option) and item["ticker"] != "SPX"), None)
            if not match:
                await ctx.send("Option not found.")
                return

            market = "{:.2f}".format(float(match.get("mid")))
            if price.lower() == "m":
                price = match.get("last")
                if price == "None":
                    price = market

            if float(price) < float(match.get("mid")) * 0.9 or float(price) > float(match.get("mid")) * 1.1 and price != match.get("last"):
                await ctx.send(f"Your price {price} for {symbol} {strike_with_type} is too far from current market {market}, use @ m or the current price")
                return
            
            direction_label, is_long = get_order_direction(ctx.invoked_with)
            if ctx.invoked_with.upper() in ["BTO", "STO"]:
                user_name = ctx.author.name
                if is_avg:
                    if type_option not in ["C", "P"]:
                        await ctx.send("AVG is only allowed for options (C or P).")
                        return
                    if not is_trade_open(user_name, symbol, formatted_date, strike_with_type[:-1], type_option):
                        await ctx.send(f"Trade {symbol} {date_str} {strike_with_type} must be open to average down.")
                        return
                    result = avg_down_trade(user_name, symbol, float(price), avg_qty, formatted_date, strike_with_type[:-1], type_option)
                    if result is None:
                        await ctx.send(f"Trade {symbol} {date_str} {strike_with_type} is not open")
                        return
                    if result is False:
                        await ctx.send(f"Cannot average down {symbol} {date_str} {strike_with_type} more than 2 times.")
                        return
                    avg_entry_price, _ = result
                    closing_price = None  # No closing price for AVG
                else:
                    result = open_trade(user_name, symbol, float(price), 1, formatted_date, strike_with_type[:-1], type_option)
                    if result is None:
                        await ctx.send(f"Trade {symbol} is already opened")
                        return
                    avg_entry_price, closing_price = result
            elif ctx.invoked_with.upper() in ["STC", "BTC"]:
                user_name = ctx.author.name
                if is_avg:
                    await ctx.send("AVG is not allowed for STC or BTC commands.")
                    return
                if is_trim:
                    if type_option not in ["C", "P"]:
                        await ctx.send("Trim is only allowed for options (C or P).")
                        return
                    result = trim_trade(user_name, symbol, float(price), formatted_date, strike_with_type[:-1], type_option)
                    if result is None:
                        await ctx.send(f"Trade {symbol} {date_str} {strike_with_type} is not open")
                        return
                    if result is False:
                        await ctx.send(f"Cannot trim {symbol} {date_str} {strike_with_type} more than 4 times. Please close the trade.")
                        return
                    avg_entry_price, _ = result
                    closing_price = float(price)  # Use trim price as closing price for display
                else:
                    result = close_trade(user_name, symbol, float(price), formatted_date, strike_with_type[:-1], type_option)
                    if result is None:
                        await ctx.send(f"Trade {symbol} {date_str} {strike_with_type} is not open")
                        return
                    avg_entry_price, closing_price = result
            await ctx.send(f"{ctx.invoked_with.upper()} {symbol} {date_str} {strike_with_type} @ {price} {'trim' if is_trim else ''}{'AVG ' + str(avg_qty) if is_avg else ''}")
            embed = build_embed(
                ctx,
                symbol,
                price,
                market,
                direction_label,
                extra=f"{date_str} {strike_with_type}",
                is_long=is_long,
                avg_entry_price=avg_entry_price,
                closing_price=closing_price,
                trim=f"{'trim' if is_trim else ''}",
                avg=f"{'AVG ' + str(avg_qty) if is_avg else ''}"
            )
            await ctx.send(embed=embed)
            return

        else:
            await ctx.send("Invalid format. Use one of:\n- BTO AAPL @ M\n- BTO SPX 8/4/25 6300P @ M AVG 230\n- STC SPX 8/4/25 6000C @ M trim")
            return

    except Exception as e:
        await ctx.send(f"Error retrieving data: {e}")

@order_command.error
async def error_type(ctx, error):
    if isinstance(error, commands.errors.MissingRequiredArgument):
        cmd = ctx.invoked_with.upper()
        await ctx.send(f"""Add arguments for ticker date strike @ price [trim | AVG qty] like this:
                       {cmd} SPX 12/25/25 7000C @ 10
                       {cmd} SPX 12/25/25 7000C @ M trim
                       {cmd} SPX 12/25/25 7000C @ M AVG 230
                       {cmd} /ES @ M
                       {cmd} TSLA @ 342.43""")

@bot.command(name="stats")
async def stats_command(ctx, username: str = None, timeframe: str = "all", status: str = "all"):
    """
    Shows detailed trading statistics.
    
    Usage:
        stats [username] [timeframe] [status]
    
    Examples:
        stats
        stats jinskukripta monthly closed
    """
    try:
        # Validate parameters
        valid_timeframes = ["today", "weekly", "monthly", "yearly", "all"]
        valid_statuses = ["open", "closed", "all"]
        
        if timeframe not in valid_timeframes:
            embed = discord.Embed(
                title="Invalid Timeframe",
                description=f"**You used:** `{timeframe}`\n**Valid options:** `{', '.join(valid_timeframes)}`\n\n**Usage:**\n`!stats [username] [timeframe] [status]`\n\n**Examples:**\n• `!stats` - your all-time stats\n• `!stats jinskukripta` - jinskukripta's all-time stats\n• `!stats jinskukripta monthly` - jinskukripta's monthly stats\n• `!stats jinskukripta monthly closed` - only closed trades",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return
        
        if status not in valid_statuses:
            embed = discord.Embed(
                title="Invalid Status",
                description=f"**You used:** `{status}`\n**Valid options:** `{', '.join(valid_statuses)}`\n\n**Usage:**\n`!stats [username] [timeframe] [status]`\n\n**Examples:**\n• `!stats jinskukripta all open` - only open trades\n• `!stats jinskukripta all closed` - only closed trades\n• `!stats jinskukripta all all` - all trades (default)",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return
        
        # Default user
        if username is None:
            username = ctx.author.name
        
        # Get trades
        trades = get_trade_stats(username, timeframe, status)
        if trades is None:
            embed = discord.Embed(
                title="Invalid Parameters",
                description=f"The timeframe or status you provided is not valid.\n\n**Valid timeframes:** `{', '.join(valid_timeframes)}`\n**Valid status:** `{', '.join(valid_statuses)}`\n\n**Usage:**\n`!stats [username] [timeframe] [status]`\n\n**Examples:**\n• `!stats` - your all-time stats\n• `!stats {username}` - all-time stats for {username}\n• `!stats {username} monthly closed` - monthly closed trades\n\n*If you see this error with valid parameters, contact bot admin.*",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return
        
        if not trades:
            embed = discord.Embed(
                title="No Trades Found",
                description=f"**User:** {username}\n**Timeframe:** {timeframe}\n**Status:** {status}\n\nNo trades match these criteria.\n\n**Try:**\n• `!stats {username}` - all trades\n• `!stats {username} all open` - only open trades\n• Check if the username is spelled correctly",
                color=discord.Color.orange()
            )
            await ctx.send(embed=embed)
            return
        
        # Convert to list of dictionaries
        trades_list = []
        for trade in trades:
            trades_list.append({
                "ticker": trade["ticker"],
                "date": trade["date"],
                "strike": trade["strike"],
                "type": trade["type"],
                "price": trade["price"],
                "qty": trade["qty"],
                "avg_down1": trade["avg_down1"],
                "avg_down1_qty": trade["avg_down1_qty"],
                "avg_down2": trade["avg_down2"],
                "avg_down2_qty": trade["avg_down2_qty"],
                "trim1": trade["trim1"],
                "trim2": trade["trim2"],
                "trim3": trade["trim3"],
                "trim4": trade["trim4"],
                "closing_price": trade["closing_price"],
                "opened": trade["opened"],
                "timestamp": trade["timestamp"],
                "closed_timestamp": trade["closed_timestamp"]
            })
        
        # Calculate improved statistics
        stats_calc = TradeStats(trades_list)
        stats_report = stats_calc.format_comprehensive_report()
        
        # Create embed with statistics
        embed = discord.Embed(
            title=f"Trading Statistics - {username}",
            description=f"**Period:** {timeframe.capitalize()} | **Status:** {status.capitalize()}\n\n{stats_report}",
            color=discord.Color.blue()
        )
        
        now_est = datetime.datetime.now(ZoneInfo("America/New_York"))
        embed.set_footer(text=f"{now_est.strftime('%Y-%m-%d %I:%M %p EST')}\nTrade Tracker Bot")
        
        await ctx.send(embed=embed)
        
        # Show detailed trade list
        # Discord has a 6000 character limit per message, so we limit to ~50 trades
        max_trades_to_show = 50
        
        if len(trades_list) > 0:
            trade_lines = []
            for i, trade in enumerate(trades_list[:max_trades_to_show], 1):
                ticker = trade["ticker"]
                
                # Calculate average entry price
                prices = [(trade["price"], trade["qty"])]
                if trade["avg_down1"]:
                    prices.append((trade["avg_down1"], trade["avg_down1_qty"]))
                if trade["avg_down2"]:
                    prices.append((trade["avg_down2"], trade["avg_down2_qty"]))
                
                total_qty = sum(q for _, q in prices)
                avg_entry = sum(p * q for p, q in prices) / total_qty if total_qty > 0 else trade["price"]
                
                # Format ticker
                if trade["date"]:
                    ticker_str = f"{ticker} {trade['date']} {trade['strike']}{trade['type']}"
                else:
                    ticker_str = ticker
                
                line = f"`{i:2d}.` {ticker_str} @ {avg_entry:.2f}"
                
                # If closed, calculate PnL
                if trade["opened"] == 0:
                    exit_prices = [p for p in [trade["trim1"], trade["trim2"], 
                                               trade["trim3"], trade["trim4"], 
                                               trade["closing_price"]] if p is not None]
                    if exit_prices:
                        avg_exit = sum(exit_prices) / len(exit_prices)
                        is_long = trade["type"] in ["L", "C"]
                        
                        if '/' in ticker:
                            pnl = (avg_exit - avg_entry) if is_long else (avg_entry - avg_exit)
                            line += f" → **{pnl:+.2f}pts**"
                        else:
                            try:
                                pnl = ((avg_exit - avg_entry) / avg_entry * 100) if is_long else \
                                      ((avg_entry - avg_exit) / avg_entry * 100)
                                line += f" → **{pnl:+.2f}%**"
                            except:
                                pass
                else:
                    line += " `[OPEN]`"
                
                trade_lines.append(line)
            
            # Add truncation message if there are more trades
            if len(trades_list) > max_trades_to_show:
                trade_lines.append(f"\n*... and {len(trades_list) - max_trades_to_show} more trades*")
            
            if trade_lines:
                # Split into multiple embeds if needed (Discord 6000 char limit)
                description = "\n".join(trade_lines)
                
                if len(description) > 4000:
                    # Split into chunks
                    mid_point = len(trade_lines) // 2
                    
                    embed2 = discord.Embed(
                        title=f"Trade Details (1/2)",
                        description="\n".join(trade_lines[:mid_point]),
                        color=discord.Color.green()
                    )
                    await ctx.send(embed=embed2)
                    
                    embed3 = discord.Embed(
                        title=f"Trade Details (2/2)",
                        description="\n".join(trade_lines[mid_point:]),
                        color=discord.Color.green()
                    )
                    await ctx.send(embed3)
                else:
                    embed2 = discord.Embed(
                        title="Trade Details",
                        description=description,
                        color=discord.Color.green()
                    )
                    await ctx.send(embed=embed2)
    
    except Exception as e:
        embed = discord.Embed(
            title="Unexpected Error",
            description=f"**Error:** `{str(e)}`\n\n**Command format:**\n`!stats [username] [timeframe] [status]`\n\n**Examples:**\n• `!stats` - your stats\n• `!stats jinskukripta` - user's stats\n• `!stats jinskukripta monthly closed`\n\n**Valid timeframes:** `today`, `weekly`, `monthly`, `yearly`, `all`\n**Valid status:** `open`, `closed`, `all`\n\nIf error persists, contact bot admin.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        import traceback
        traceback.print_exc()

@tasks.loop(time=datetime.time(hour=16, minute=15, tzinfo=ZoneInfo("America/New_York")))
async def close_expiring_options():
    """Close all open options trades expiring on or before today at 16:15 EST."""
    try:
        trades = get_open_options_expiring_today()
        if not trades:
            return  # No trades to close

        # Get the channel to send notifications
        channel_id = DISCORD_CHANNEL_ID
        if channel_id:
            channel = bot.get_channel(int(channel_id))
        else:
            # Fallback to the first text channel
            channel = next((c for c in bot.get_all_channels() if isinstance(c, discord.TextChannel)), None)
        if not channel:
            print("No valid Discord channel found for notifications.")
            return

        for i in range(len(trades)):
            user = trades[i][0]
            ticker = trades[i][1]
            date = trades[i][2]
            strike = trades[i][3]
            type_opt = trades[i][4]
            opening_price = trades[i][5]

            # Fetch the last price from Tastytrade
            exp_date = parse_option_date(date)
            options_request = {
                "tickers": [ticker],
                "start_date": exp_date,
                "end_date": exp_date,
                "lower_strike": str(strike),
                "upper_strike": str(float(strike) + 1)
            }
            data, _ = await tasty_data(session, options_requested=options_request)

            def is_type_option(symbol: str, type_option: str) -> bool:
                i = len(symbol) - 1
                while i >= 0 and symbol[i].isdigit():
                    i -= 1
                suffix = symbol[i:].lower()
                return type_option.lower() in suffix

            
            match = next((item for item in data if str(strike) in item["strike"] and is_type_option(item["symbol"], type_option)), None)
            if not match:
                print(f"Option {ticker} {date} {strike}{type_opt} not found in Tastytrade.")
                closing_price = 0
                market = "0.00"
            else:
                closing_price = match.get("last")
                if closing_price == "None":
                    closing_price = float(match.get("mid"))
                else:
                    closing_price = float(closing_price)
                market = "{:.2f}".format(float(match.get("mid")))

            # Close the trade
            result = close_trade(user, ticker, closing_price, date, strike, type_opt)
            if result is None:
                print(f"Failed to close trade {ticker} {date} {strike}{type_opt} for {user}.")
                continue

            # Send notification
            avg_entry_price, closing_price = result
            command = "STC"
            direction_label, is_long = get_order_direction(command)
            embed = build_embed(
                None,  # No ctx, system-initiated
                ticker,
                closing_price,
                market,
                direction_label,
                extra=f"{date} {strike}{type_opt}",
                is_long=is_long,
                avg_entry_price=avg_entry_price,
                closing_price=closing_price
            )
            await channel.send(f"{command} {ticker} {date} {strike}{type_opt} @ {closing_price:.2f}")
            await channel.send(embed=embed)

    except Exception as e:
        print(f"Error in close_expiring_options: {e}")

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, CommandNotFound):
        return
    else:
        raise error

@bot.event
async def on_ready():
    print(f"Logged in {bot.user}")
    print("Verificando trades expirados al inicio...")
    await close_expiring_options()
    if not close_expiring_options.is_running():
        close_expiring_options.start()

bot.run(DISCORD_TOKEN)
