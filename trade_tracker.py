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

load_dotenv()
DISCORD_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
TASTYTRADE_USERNAME = os.getenv("TASTYTRADE_USERNAME")
TASTYTRADE_PASSWORD = os.getenv("TASTYTRADE_PASSWORD")
DISCORD_CHANNEL_ID = os.getenv("DISCORD_CHANNEL_ID")  # Optional: Channel ID for notifications

session = Session(TASTYTRADE_USERNAME, TASTYTRADE_PASSWORD)

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='', intents=intents, case_insensitive=True)

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
    footer_text = f"\n{formatted_time}\nTrade Tracker Bot"

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
            match = next((item for item in data if item["strike"] == options_request["lower_strike"] and type_option in item["symbol"]), None)
            if not match:
                await ctx.send("Option not found.")
                return

            market = "{:.2f}".format(float(match.get("mid")))
            if price.lower() == "m":
                price = match.get("last")
                if price == "None":
                    price = market

            if float(price) < float(match.get("mid")) * 0.9 or float(price) > float(match.get("mid")) * 1.1:
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

@bot.command(name="#stats")
async def stats_command(ctx, *, args):
    try:
        # Parse arguments: expecting "<username> <timeframe> <status>"
        parts = args.split(" ")
        if len(parts) != 3:
            await ctx.send("Invalid format. Use: stats <username> <today/weekly/monthly/yearly> <open/closed/all>")
            return

        username = parts[0]
        timeframe = parts[1].lower()
        status = parts[2].lower()

        # Validate timeframe and status
        if timeframe not in ["today", "weekly", "monthly", "yearly"]:
            await ctx.send("Invalid timeframe. Use: today, weekly, monthly, or yearly")
            return
        if status not in ["open", "closed", "all"]:
            await ctx.send("Invalid stats type. Use: open, closed, or all")
            return

        # Fetch trades from database
        trades = get_trade_stats(username, timeframe, status)
        if trades is None:
            await ctx.send(f"Error fetching stats for {username}. Invalid parameters or database issue.")
            return
        if not trades:
            await ctx.send(f"No {status} trades found for {username} in the {timeframe} timeframe.")
            return

        # Build trade list
        trade_list = []
        for trade in trades:
            ticker = trade["ticker"]
            date = trade["date"] if trade["date"] else ""
            strike = trade["strike"] if trade["strike"] else ""
            price = trade["price"]
            qty = trade["qty"]
            is_long = trade["type"] == "L" or trade["type"] == "C"
            timestamp = datetime.datetime.strptime(trade["timestamp"], "%Y-%m-%d %H:%M:%S").strftime("%m/%d/%Y")
            # Calculate average entry price
            prices = [(price, qty)]
            if trade["avg_down1"] is not None:
                prices.append((trade["avg_down1"], trade["avg_down1_qty"]))
            if trade["avg_down2"] is not None:
                prices.append((trade["avg_down2"], trade["avg_down2_qty"]))
            total_qty = sum(q for _, q in prices)
            avg_entry_price = sum(p * q for p, q in prices) / total_qty if total_qty > 0 else price

            if not trade["date"]:
                trade_str = f"{ticker} {strike} @ {avg_entry_price:.2f} ({timestamp})"
            else:
                trade_str = f"{ticker} {date} {strike}{trade['type']} @ {avg_entry_price:.2f}"

            if trade["opened"] == 0:
                try:
                    prices = [p for p in [trade["trim1"], trade["trim2"], trade["trim3"], trade["trim4"], trade["closing_price"]] if p is not None]
                    avg_exit_price = sum(prices) / len(prices) if prices else None

                    if avg_exit_price is not None:
                        if '/' in ticker:
                            change = (avg_exit_price - avg_entry_price) if is_long else (avg_entry_price - avg_exit_price)
                            trade_str += f" PnL: {'+' if change >= 0 else '-'}{abs(change):.2f}pts"
                        else:
                            change = ((avg_exit_price - avg_entry_price) / avg_entry_price * 100) if is_long else ((avg_entry_price - avg_exit_price) / avg_entry_price * 100)
                            trade_str += f" PnL: {'+' if change >= 0 else '-'}{abs(change):.2f}%"
                    else:
                        trade_str += " PnL: N/A"
                except ZeroDivisionError:
                    trade_str += " PnL: N/A"
            else:
                # Include trims and avg-downs for open trades
                trims = [p for p in [trade["trim1"], trade["trim2"], trade["trim3"], trade["trim4"]] if p is not None]
                if trims:
                    trade_str += f" Trims: {', '.join(f'{p:.2f}' for p in trims)}"
                avg_downs = [p for p in [(trade["avg_down1"], trade["avg_down1_qty"]), (trade["avg_down2"], trade["avg_down2_qty"])] if p[0] is not None]
                if avg_downs:
                    trade_str += f" Avg Downs: {', '.join(f'{p:.2f} ({q})' for p, q in avg_downs)}"
                trade_str += " (OPEN)"
            
            trade_list.append(trade_str)

        # Limit trade list to avoid exceeding Discord's 6000-character limit
        max_trades = 40  # Adjust based on testing
        if len(trade_list) > max_trades:
            trade_list = trade_list[:max_trades]
            trade_list.append(f"... (truncated, {len(trades) - max_trades} more trades)")

        # Calculate average PnL for closed trades or all trades (closed only)
        avg_pnl_options = None
        avg_pnl_stocks = None
        avg_pnl_futures = None
        if status in ["closed", "all"]:
            options_pnl = []
            stocks_pnl = []
            futures_pnl = []
            for trade in trades:
                if trade["opened"] == 1:
                    continue
                try:
                    price = trade["price"]
                    qty = trade["qty"]
                    prices = [(price, qty)]
                    if trade["avg_down1"] is not None:
                        prices.append((trade["avg_down1"], trade["avg_down1_qty"]))
                    if trade["avg_down2"] is not None:
                        prices.append((trade["avg_down2"], trade["avg_down2_qty"]))
                    total_qty = sum(q for _, q in prices)
                    avg_entry_price = sum(p * q for p, q in prices) / total_qty if total_qty > 0 else price
                    is_long = trade["type"] == "L" or trade["type"] == "C"
                    if '/' in trade["ticker"]:
                        prices = [p for p in [trade["trim1"], trade["trim2"], trade["trim3"], trade["trim4"], trade["closing_price"]] if p is not None]
                        avg_exit_price = sum(prices) / len(prices) if prices else None
                        if avg_exit_price is not None:
                            change = (avg_exit_price - avg_entry_price) if is_long else (avg_entry_price - avg_exit_price)
                            futures_pnl.append(change)
                    elif trade["type"] in ["C", "P"]:
                        prices = [p for p in [trade["trim1"], trade["trim2"], trade["trim3"], trade["trim4"], trade["closing_price"]] if p is not None]
                        avg_exit_price = sum(prices) / len(prices) if prices else None
                        if avg_exit_price is not None:
                            change = ((avg_exit_price - avg_entry_price) / avg_entry_price * 100) if is_long else ((avg_entry_price - avg_exit_price) / avg_entry_price * 100)
                            options_pnl.append(change)
                    elif trade["type"] in ["S", "L"]:
                        prices = [p for p in [trade["trim1"], trade["trim2"], trade["trim3"], trade["trim4"], trade["closing_price"]] if p is not None]
                        avg_exit_price = sum(prices) / len(prices) if prices else None
                        if avg_exit_price is not None:
                            change = ((avg_exit_price - avg_entry_price) / avg_entry_price * 100) if is_long else ((avg_entry_price - avg_exit_price) / avg_entry_price * 100)
                            stocks_pnl.append(change)
                except (ZeroDivisionError, TypeError):
                    continue

            avg_pnl_options = sum(options_pnl) / len(options_pnl) if options_pnl else None
            avg_pnl_stocks = sum(stocks_pnl) / len(stocks_pnl) if stocks_pnl else None
            avg_pnl_futures = sum(futures_pnl) / len(futures_pnl) if futures_pnl else None

        # Build description
        description = f"**User**: {username}\n**Timeframe**: {timeframe.capitalize()}\n**Status**: {status.capitalize()}\n**Total Trades**: {len(trades)}\n\n"
        description += "**Trades**:\n" + "\n".join(trade_list) + "\n\n"
        
        if status in ["closed", "all"]:
            description += "**Average PnL**:\n"
            if avg_pnl_options:
                description += f"- Options (C/P): {'+' if avg_pnl_options >= 0 else '-'}{abs(avg_pnl_options):.2f}%\n"
            else:
                description += "- Options (C/P): N/A\n"
            if avg_pnl_stocks:
                description += f"- Stocks (S/L): {'+' if avg_pnl_stocks >= 0 else '-'}{abs(avg_pnl_stocks):.2f}%\n"
            else:
                description += "- Stocks (S/L): N/A\n"
            if avg_pnl_futures:
                description += f"- Futures: {'+' if avg_pnl_futures >= 0 else '-'}{abs(avg_pnl_futures):.2f}pts\n"
            else:
                description += "- Futures: N/A\n"

        # Build embed
        embed = discord.Embed(
            title=f"Trade Statistics for {username}",
            description=description,
            color=discord.Color.blue()
        )
        now_est = datetime.datetime.now(ZoneInfo("America/New_York"))
        embed.set_footer(text=f"{now_est.strftime('%Y-%m-%d %I:%M %p EST')}\nTrade Tracker Bot")
        await ctx.send(embed=embed)

    except Exception as e:
        await ctx.send(f"Error retrieving stats: {e}")

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
            exp_date = datetime.datetime.strptime(date, "%m/%d/%y").date()
            options_request = {
                "tickers": [ticker],
                "start_date": exp_date,
                "end_date": exp_date,
                "lower_strike": str(strike),
                "upper_strike": str(float(strike) + 1)
            }
            data, _ = await tasty_data(session, options_request=options_request)
            match = next((item for item in data if str(strike) in item["strike"] and type_opt in item["symbol"]), None)
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
            command = "STC" if type_opt == "C" else "BTC"
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
    if not close_expiring_options.is_running():
        close_expiring_options.start()

bot.run(DISCORD_TOKEN)



