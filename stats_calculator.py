"""
stats_calculator.py - Advanced statistics calculator for trades
"""
from typing import List, Dict, Tuple
from datetime import datetime

class TradeStats:
    """Calculates detailed trade statistics"""
    
    def __init__(self, trades: List[Dict]):
        self.trades = trades
        self.closed_trades = [t for t in trades if t["opened"] == 0]
        self.open_trades = [t for t in trades if t["opened"] == 1]
    
    def _get_total_position_size(self, trade: Dict) -> float:
            """Helper: Calcula la cantidad total comprada (Inicial + Avg Downs)"""
            qty = trade.get("qty", 0)
            if trade.get("avg_down1") is not None:
                qty += trade.get("avg_down1_qty", 0)
            if trade.get("avg_down2") is not None:
                qty += trade.get("avg_down2_qty", 0)
            return qty
    
    def _calculate_entry_price(self, trade: Dict) -> float:
        """Calculates average entry price including avg-downs (Ponderado)"""
        # Nota: Usamos .get() para evitar errores si falta alguna clave
        prices = [(trade.get("price"), trade.get("qty", 0))]
        
        if trade.get("avg_down1") is not None:
            prices.append((trade.get("avg_down1"), trade.get("avg_down1_qty", 0)))
        if trade.get("avg_down2") is not None:
            prices.append((trade.get("avg_down2"), trade.get("avg_down2_qty", 0)))
        
        total_qty = sum(q for _, q in prices)
        return sum(p * q for p, q in prices) / total_qty if total_qty > 0 else trade["price"]

    def _calculate_exit_price(self, trade: Dict) -> float:
        """
        Calculates WEIGHTED average exit price.
        Logic: (Trim1*Qty1 + Trim2*Qty2 + Close*RemainingQty) / TotalQty
        """
        total_qty = self._get_total_position_size(trade)
        
        if total_qty == 0:
            return None

        exits = []
        qty_sold_so_far = 0

        # 1. Procesar Trims (Parciales)
        # Busca trim1 y trim1_qty, trim2 y trim2_qty, etc.
        for i in range(1, 5):
            p_key = f"trim{i}"
            q_key = f"trim{i}_qty"
            
            t_price = trade.get(p_key)
            # Si no existe la key _qty en el dict, asumimos 0 (esto es una protección)
            t_qty = trade.get(q_key, 0)
            
            if t_price is not None:
                # Si el usuario olvidó poner trim_qty pero puso precio, 
                # esto podría causar error de cálculo. Asumimos que los datos están bien.
                exits.append((t_price, t_qty))
                qty_sold_so_far += t_qty

        # 2. Procesar Cierre Final (Closing Price)
        # La cantidad de cierre es lo que sobra del total menos los trims
        closing_price = trade.get("closing_price")
        remaining_qty = total_qty - qty_sold_so_far
        
        if closing_price is not None:
            # Si remaining_qty es 0 o negativo, algo anda mal con los datos, 
            # pero matemáticamente solo sumamos si queda algo.
            if remaining_qty > 0:
                exits.append((closing_price, remaining_qty))
            elif not exits: 
                # Si no hubo trims y remaining es 0 (error de data), devolvemos precio de cierre simple
                return closing_price

        if not exits:
            return None

        # 3. Calcular Promedio Ponderado
        total_exit_value = sum(p * q for p, q in exits)
        total_exit_qty = sum(q for _, q in exits)
        
        if total_exit_qty == 0:
            return None
            
        return total_exit_value / total_exit_qty
        
    def _calculate_pnl(self, trade: Dict) -> Tuple[float, str]:
        """Calculates trade PnL (value, type)"""
        avg_entry = self._calculate_entry_price(trade)
        avg_exit = self._calculate_exit_price(trade)
        
        if avg_exit is None:
            return None, None
        
        is_long = trade["type"] in ["L", "C"]
        ticker = trade["ticker"]
        
        # Futures (points)
        if '/' in ticker:
            pnl = (avg_exit - avg_entry) if is_long else (avg_entry - avg_exit)
            return pnl, "pts"
        # Options and Stocks (percentage)
        else:
            try:
                pnl = ((avg_exit - avg_entry) / avg_entry * 100) if is_long else \
                      ((avg_entry - avg_exit) / avg_entry * 100)
                return pnl, "%"
            except ZeroDivisionError:
                return None, None
    
    def get_basic_stats(self) -> Dict:
        """Basic statistics"""
        return {
            "total_trades": len(self.trades),
            "open_trades": len(self.open_trades),
            "closed_trades": len(self.closed_trades),
        }
    
    def get_pnl_by_type(self) -> Dict:
        """Average PnL by instrument type"""
        options_pnl = []
        stocks_pnl = []
        futures_pnl = []
        
        for trade in self.closed_trades:
            pnl, pnl_type = self._calculate_pnl(trade)
            if pnl is None:
                continue
            
            # Check futures FIRST (by ticker), then options (by type)
            if '/' in trade["ticker"]:
                futures_pnl.append(pnl)
            elif trade["type"] in ["C", "P"]:
                options_pnl.append(pnl)
            elif trade["type"] in ["S", "L"]:
                stocks_pnl.append(pnl)
        
        return {
            "options": {
                "avg": sum(options_pnl) / len(options_pnl) if options_pnl else None,
                "count": len(options_pnl),
                "total": sum(options_pnl) if options_pnl else 0,
                "wins": sum(1 for p in options_pnl if p > 0),
                "losses": sum(1 for p in options_pnl if p < 0),
            },
            "stocks": {
                "avg": sum(stocks_pnl) / len(stocks_pnl) if stocks_pnl else None,
                "count": len(stocks_pnl),
                "total": sum(stocks_pnl) if stocks_pnl else 0,
                "wins": sum(1 for p in stocks_pnl if p > 0),
                "losses": sum(1 for p in stocks_pnl if p < 0),
            },
            "futures": {
                "avg": sum(futures_pnl) / len(futures_pnl) if futures_pnl else None,
                "count": len(futures_pnl),
                "total": sum(futures_pnl) if futures_pnl else 0,
                "wins": sum(1 for p in futures_pnl if p > 0),
                "losses": sum(1 for p in futures_pnl if p < 0),
            }
        }
    
    def get_win_rate(self) -> Dict:
        """Calculates win rate by category"""
        pnl_stats = self.get_pnl_by_type()
        
        def calc_win_rate(stats):
            total = stats["wins"] + stats["losses"]
            if total == 0:
                return None
            return (stats["wins"] / total) * 100
        
        return {
            "options": calc_win_rate(pnl_stats["options"]),
            "stocks": calc_win_rate(pnl_stats["stocks"]),
            "futures": calc_win_rate(pnl_stats["futures"]),
            "overall": calc_win_rate({
                "wins": sum(pnl_stats[k]["wins"] for k in ["options", "stocks", "futures"]),
                "losses": sum(pnl_stats[k]["losses"] for k in ["options", "stocks", "futures"])
            })
        }
    
    def get_best_worst_trades(self, limit=3) -> Dict:
        """Returns best and worst trades"""
        trades_with_pnl = []
        
        for trade in self.closed_trades:
            pnl, pnl_type = self._calculate_pnl(trade)
            if pnl is not None:
                trades_with_pnl.append({
                    "trade": trade,
                    "pnl": pnl,
                    "pnl_type": pnl_type
                })
        
        if not trades_with_pnl:
            return {"best": [], "worst": []}
        
        # Sort by PnL
        sorted_trades = sorted(trades_with_pnl, key=lambda x: x["pnl"], reverse=True)
        
        return {
            "best": sorted_trades[:limit],
            "worst": sorted_trades[-limit:][::-1]  # Reverse to show worst to better
        }
    
    def get_trading_activity(self) -> Dict:
        """Trading activity analysis"""
        if not self.trades:
            return {
                "most_traded_ticker": None,
                "trades_per_ticker": {},
                "avg_downs_used": 0,
                "trims_used": 0,
            }
        
        # Count trades per ticker
        ticker_counts = {}
        total_avg_downs = 0
        total_trims = 0
        
        for trade in self.trades:
            ticker = trade["ticker"]
            ticker_counts[ticker] = ticker_counts.get(ticker, 0) + 1
            
            # Count avg-downs
            if trade["avg_down1"] is not None:
                total_avg_downs += 1
            if trade["avg_down2"] is not None:
                total_avg_downs += 1
            
            # Count trims
            for trim in [trade["trim1"], trade["trim2"], trade["trim3"], trade["trim4"]]:
                if trim is not None:
                    total_trims += 1
        
        most_traded = max(ticker_counts.items(), key=lambda x: x[1]) if ticker_counts else (None, 0)
        
        return {
            "most_traded_ticker": most_traded[0],
            "most_traded_count": most_traded[1],
            "trades_per_ticker": ticker_counts,
            "avg_downs_used": total_avg_downs,
            "trims_used": total_trims,
            "unique_tickers": len(ticker_counts),
        }
    
    def get_time_analysis(self) -> Dict:
        """Temporal analysis of trades"""
        if not self.closed_trades:
            return {
                "avg_hold_time_hours": None,
                "avg_hold_time_days": None,
                "shortest_trade": None,
                "longest_trade": None,
            }
        
        hold_times = []
        
        for trade in self.closed_trades:
            if trade["timestamp"] and trade["closed_timestamp"]:
                try:
                    opened = datetime.strptime(trade["timestamp"], "%Y-%m-%d %H:%M:%S")
                    closed = datetime.strptime(trade["closed_timestamp"], "%Y-%m-%d %H:%M:%S")
                    hold_time = (closed - opened).total_seconds() / 3600  # hours
                    hold_times.append({
                        "trade": trade,
                        "hours": hold_time
                    })
                except:
                    continue
        
        if not hold_times:
            return {
                "avg_hold_time_hours": None,
                "avg_hold_time_days": None,
                "shortest_trade": None,
                "longest_trade": None,
            }
        
        avg_hours = sum(t["hours"] for t in hold_times) / len(hold_times)
        shortest = min(hold_times, key=lambda x: x["hours"])
        longest = max(hold_times, key=lambda x: x["hours"])
        
        return {
            "avg_hold_time_hours": avg_hours,
            "avg_hold_time_days": avg_hours / 24,
            "shortest_trade": shortest,
            "longest_trade": longest,
        }
    
    def format_comprehensive_report(self) -> str:
        """Generates a comprehensive report in readable format"""
        basic = self.get_basic_stats()
        pnl = self.get_pnl_by_type()
        win_rate = self.get_win_rate()
        activity = self.get_trading_activity()
        time_stats = self.get_time_analysis()
        best_worst = self.get_best_worst_trades(limit=3)
        
        report = []
        
        # Basic section
        report.append("📊 **OVERVIEW**")
        report.append(f"Total trades: {basic['total_trades']}")
        report.append(f"├─ Open: {basic['open_trades']}")
        report.append(f"└─ Closed: {basic['closed_trades']}")
        
        if basic['closed_trades'] > 0:
            # Win rate
            report.append("")
            report.append("🎯 **WIN RATE**")
            if win_rate["overall"]:
                report.append(f"Overall: {win_rate['overall']:.1f}%")
            if win_rate["options"] and pnl["options"]["count"] > 0:
                report.append(f"├─ Options: {win_rate['options']:.1f}% ({pnl['options']['wins']}W/{pnl['options']['losses']}L)")
            if win_rate["stocks"] and pnl["stocks"]["count"] > 0:
                report.append(f"├─ Stocks: {win_rate['stocks']:.1f}% ({pnl['stocks']['wins']}W/{pnl['stocks']['losses']}L)")
            if win_rate["futures"] and pnl["futures"]["count"] > 0:
                report.append(f"└─ Futures: {win_rate['futures']:.1f}% ({pnl['futures']['wins']}W/{pnl['futures']['losses']}L)")
            
            # PnL
            report.append("")
            report.append("💰 **AVERAGE PNL**")
            if pnl["options"]["avg"] is not None and pnl["options"]["count"] > 0:
                sign = "+" if pnl["options"]["avg"] >= 0 else ""
                report.append(f"Options: {sign}{pnl['options']['avg']:.2f}% (Total: {sign}{pnl['options']['total']:.2f}%)")
            if pnl["stocks"]["avg"] is not None and pnl["stocks"]["count"] > 0:
                sign = "+" if pnl["stocks"]["avg"] >= 0 else ""
                report.append(f"Stocks: {sign}{pnl['stocks']['avg']:.2f}% (Total: {sign}{pnl['stocks']['total']:.2f}%)")
            if pnl["futures"]["avg"] is not None and pnl["futures"]["count"] > 0:
                sign = "+" if pnl["futures"]["avg"] >= 0 else ""
                report.append(f"Futures: {sign}{pnl['futures']['avg']:.2f}pts (Total: {sign}{pnl['futures']['total']:.2f}pts)")
        
        # Activity
        report.append("")
        report.append("📈 **ACTIVITY**")
        if activity["most_traded_ticker"]:
            report.append(f"Most traded: {activity['most_traded_ticker']} ({activity['most_traded_count']} trades)")
        report.append(f"Unique tickers: {activity['unique_tickers']}")
        report.append(f"Avg-downs used: {activity['avg_downs_used']}")
        report.append(f"Trims executed: {activity['trims_used']}")
        
        # Time (only show if there are closed trades with valid timestamps)
        if time_stats.get("avg_hold_time_hours") is not None:
            report.append("")
            report.append("⏱️ **HOLD TIME**")
            if time_stats["avg_hold_time_hours"] < 24:
                report.append(f"Average: {time_stats['avg_hold_time_hours']:.1f} hours")
            else:
                report.append(f"Average: {time_stats['avg_hold_time_days']:.1f} days")
        
        # Best and worst trades
        if best_worst["best"]:
            report.append("")
            report.append("🏆 **BEST TRADES**")
            for i, item in enumerate(best_worst["best"], 1):
                t = item["trade"]
                pnl_str = f"+{item['pnl']:.2f}{item['pnl_type']}"
                ticker_str = f"{t['ticker']}"
                if t['date']:
                    ticker_str += f" {t['date']} {t['strike']}{t['type']}"
                report.append(f"{i}. {ticker_str}: {pnl_str}")
        
        if best_worst["worst"]:
            report.append("")
            report.append("📉 **WORST TRADES**")
            for i, item in enumerate(best_worst["worst"], 1):
                t = item["trade"]
                pnl_str = f"{item['pnl']:.2f}{item['pnl_type']}"
                ticker_str = f"{t['ticker']}"
                if t['date']:
                    ticker_str += f" {t['date']} {t['strike']}{t['type']}"
                report.append(f"{i}. {ticker_str}: {pnl_str}")
        
        return "\n".join(report)
