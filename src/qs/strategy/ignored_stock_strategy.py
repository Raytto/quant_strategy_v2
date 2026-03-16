"""Legacy research-only strategy prototype.

This module is intentionally kept out of the formal backtesting framework path.
It contains a hand-written simulator and historical shortcuts that are not
compatible with the framework-first rules in `README.md`.

买在无人问津时策略 (Buy When Ignored)

策略逻辑：
- 每3个月轮动一次
- 选5只"最无人问津"的A股
- 筛选条件：
  1. 近3个月交易额占近36个月比例最低
  2. 未停牌（有近期交易数据）
  3. 股价在36个月最高最低区间不超过5倍
  4. 当前市值>100亿
- 等权买入
"""

from __future__ import annotations

import csv
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
import time

from ..sqlite_utils import connect_sqlite
from ..backtester.data import Bar, DataFeed
from ..backtester.broker import Broker
from ..backtester.engine import BacktestEngine


@dataclass
class StockSnapshot:
    ts_code: str
    name: str
    trade_date: str
    close: float
    vol_3m: float  # 3个月成交额
    vol_36m: float  # 36个月成交额
    vol_ratio: float  # vol_3m / vol_36m
    price_36m_high: float
    price_36m_low: float
    price_range_ratio: float  # (current - low) / (high - low)
    market_cap: float  # 亿元


class IgnoredStockStrategy:
    def __init__(
        self,
        db_path_raw: str = "data/data.sqlite",
        start_date: str = "20160101",
        end_date: str = "20260301",
        rebalance_months: int = 3,
        top_k: int = 5,
        min_market_cap: float = 100.0,  # 亿元
        max_price_range: float = 5.0,  # 股价在36个月区间不超过5倍
        initial_cash: float = 1_000_000.0,
    ):
        raise RuntimeError(
            "IgnoredStockStrategy is legacy research code and is not compatible "
            "with qs.backtester. Write new strategies with StrategyContext and "
            "framework-managed market data instead."
        )
        self.dbr = db_path_raw
        self.start_date = start_date
        self.end_date = end_date
        self.rebalance_months = rebalance_months
        self.top_k = top_k
        self.min_market_cap = min_market_cap
        self.max_price_range = max_price_range
        self.initial_cash = initial_cash
        
        self._last_rebalance_date: Optional[str] = None
        self._current_positions: Set[str] = set()
        self._market_cap_cache: Dict[str, float] = {}
        
        print(f"[IgnoredStockStrategy] init: start={start_date}, end={end_date}, "
              f"rebalance={rebalance_months}m, top_k={top_k}, min_mcap={min_market_cap}B")
    
    def _load_market_cap(self, date_str: str) -> Dict[str, float]:
        """从pe数据加载市值数据，返回 {ts_code: market_cap}"""
        # 尝试从pe_latest_a.csv加载（简化处理：使用最新数据）
        # 实际回测中应该用历史pe数据
        pe_path = Path(self.dbr).parent / "pe_latest_a.csv"
        mcaps = {}
        if pe_path.exists():
            with pe_path.open(newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        mcap = float(row.get("total_mv", 0))
                        if mcap > 0:
                            mcaps[row["ts_code"]] = mcap
                    except:
                        pass
        return mcaps
    
    def _get_volume_stats(
        self, conn: sqlite3.Connection, ts_code: str, 
        end_date: str, months_3: int, months_36: int
    ) -> Optional[StockSnapshot]:
        """获取单只股票的成交量统计数据"""
        
        # 计算日期范围
        end_dt = datetime.strptime(end_date, "%Y%m%d")
        
        # 3个月前
        start_3m = (end_dt - timedelta(days=months_3 * 30)).strftime("%Y%m%d")
        # 36个月前
        start_36m = (end_dt - timedelta(days=months_36 * 30)).strftime("%Y%m%d")
        
        # 获取3个月成交额
        query_3m = """
            SELECT SUM(amount) as vol_3m, COUNT(*) as days
            FROM daily_a 
            WHERE ts_code = ? AND trade_date > ? AND trade_date <= ?
        """
        cur = conn.execute(query_3m, (ts_code, start_3m, end_date))
        row_3m = cur.fetchone()
        if not row_3m or row_3m[1] < 10:  # 至少10个交易日
            return None
        
        # 获取36个月成交额
        query_36m = """
            SELECT SUM(amount) as vol_36m
            FROM daily_a 
            WHERE ts_code = ? AND trade_date > ? AND trade_date <= ?
        """
        cur = conn.execute(query_36m, (ts_code, start_36m, end_date))
        row_36m = cur.fetchone()
        if not row_36m or row_36m[0] is None or row_36m[0] == 0:
            return None
        
        vol_3m = float(row_3m[0]) / 1e8  # 转换为亿元
        vol_36m = float(row_36m[0]) / 1e8
        
        if vol_36m == 0:
            return None
            
        # 获取36个月高低价
        query_hl = """
            SELECT MAX(high) as high_36m, MIN(low) as low_36m
            FROM daily_a 
            WHERE ts_code = ? AND trade_date > ? AND trade_date <= ?
        """
        cur = conn.execute(query_hl, (ts_code, start_36m, end_date))
        row_hl = cur.fetchone()
        if not row_hl or not row_hl[0]:
            return None
        
        high_36m = float(row_hl[0])
        low_36m = float(row_hl[1])
        
        if high_36m == low_36m or high_36m == 0:
            return None
        
        # 获取最新收盘价
        query_close = """
            SELECT close FROM daily_a 
            WHERE ts_code = ? AND trade_date <= ? 
            ORDER BY trade_date DESC LIMIT 1
        """
        cur = conn.execute(query_close, (ts_code, end_date))
        row_close = cur.fetchone()
        if not row_close:
            return None
        close = float(row_close[0])
        
        # 获取股票名称
        query_name = """
            SELECT name FROM stock_basic_a WHERE ts_code = ?
        """
        cur = conn.execute(query_name, (ts_code,))
        row_name = cur.fetchone()
        name = row_name[0] if row_name else ts_code
        
        # 计算价格区间比例 (current - low) / (high - low)
        # 归一化到0-1，越接近0越接近36个月低点，越接近1越接近高点
        price_range_ratio = (close - low_36m) / (high_36m - low_36m)
        
        # 检查是否在5倍区间内（也就是ratio在0.2-1.0之间，因为1/5=0.2）
        # 实际上条件是：股价不超过36个月最低的5倍，即 high/low < 5
        # 这等价于：current/low < 5 且 high/current < 5
        # 即 price_range_ratio 需要在合理范围内
        
        return StockSnapshot(
            ts_code=ts_code,
            name=name,
            trade_date=end_date,
            close=close,
            vol_3m=vol_3m,
            vol_36m=vol_36m,
            vol_ratio=vol_3m / vol_36m if vol_36m > 0 else float('inf'),
            price_36m_high=high_36m,
            price_36m_low=low_36m,
            price_range_ratio=price_range_ratio,
            market_cap=0.0,  # 暂时设为0，后面填充
        )
    
    def _select_ignored_stocks(
        self, conn: sqlite3.Connection, rebalance_date: str
    ) -> List[StockSnapshot]:
        """选择最无人问津的股票"""
        
        # 加载市值数据
        market_caps = self._load_market_cap(rebalance_date)
        
        # 获取所有A股
        cur = conn.execute("""
            SELECT ts_code FROM stock_basic_a 
            WHERE list_status = 'L' 
            AND ts_code NOT LIKE 'ST%' 
            AND ts_code NOT LIKE '*ST%'
        """)
        all_stocks = [row[0] for row in cur.fetchall()]
        
        print(f"  Total stocks to analyze: {len(all_stocks)}")
        
        # 筛选并计算每个股票的指标
        candidates = []
        for i, ts_code in enumerate(all_stocks):
            if i % 500 == 0:
                print(f"    Processing {i}/{len(all_stocks)}...")
            
            # 检查市值
            mcap = market_caps.get(ts_code, 0)
            if mcap < self.min_market_cap:
                continue
            
            # 获取统计信息
            snapshot = self._get_volume_stats(
                conn, ts_code, rebalance_date, 3, 36
            )
            if not snapshot:
                continue
            
            # 筛选条件：
            # 1. 股价在36个月区间不超过5倍 (high/low < 5)
            price_ratio = snapshot.price_36m_high / snapshot.price_36m_low
            if price_ratio >= self.max_price_range:
                continue
            
            # 2. 价格不能太高（接近36个月高点）- 排除在90%分位以上的
            if snapshot.price_range_ratio > 0.9:
                continue
            
            snapshot.market_cap = mcap
            candidates.append(snapshot)
        
        print(f"  Candidates after filtering: {len(candidates)}")
        
        # 按交易量比例排序（越低越无人问津）
        candidates.sort(key=lambda x: x.vol_ratio)
        
        # 返回top_k
        return candidates[:self.top_k]
    
    def _is_rebalance_date(self, trade_date: str) -> bool:
        """判断是否为调仓日（每3个月）"""
        dt = datetime.strptime(trade_date, "%Y%m%d")
        
        # 检查是否到了新的调仓周期
        if self._last_rebalance_date is None:
            return True
        
        last_dt = datetime.strptime(self._last_rebalance_date, "%Y%m%d")
        months_diff = (dt.year - last_dt.year) * 12 + (dt.month - last_dt.month)
        
        return months_diff >= self.rebalance_months
    
    def run_backtest(self) -> Dict[str, Any]:
        """运行回测"""
        
        print(f"\n=== Starting Backtest ===")
        print(f"Period: {self.start_date} to {self.end_date}")
        
        # 连接数据库
        conn = connect_sqlite(self.dbr, read_only=True)
        
        # 获取所有交易日
        cur = conn.execute("""
            SELECT DISTINCT trade_date FROM daily_a 
            WHERE trade_date >= ? AND trade_date <= ?
            ORDER BY trade_date
        """, (self.start_date, self.end_date))
        trading_days = [row[0] for row in cur.fetchall()]
        
        print(f"Total trading days: {len(trading_days)}")
        
        # 初始化
        cash = self.initial_cash
        positions: Dict[str, float] = {}  # {ts_code: shares}
        equity_curve = []
        rebalance_log = []
        
        # 模拟交易
        for i, trade_date in enumerate(trading_days):
            # 尝试调仓
            if self._is_rebalance_date(trade_date):
                print(f"\n--- Rebalance at {trade_date} ---")
                
                # 选股
                selected = self._select_ignored_stocks(conn, trade_date)
                
                if selected:
                    # 打印选股结果
                    print(f"  Selected {len(selected)} stocks:")
                    for s in selected:
                        print(f"    {s.ts_code} {s.name}: vol_ratio={s.vol_ratio:.4f}, "
                              f"price_range={s.price_range_ratio:.2f}, mcap={s.market_cap:.0f}B")
                    
                    # 计算目标持仓
                    target_stocks = {s.ts_code for s in selected}
                    
                    # 获取当前价格
                    current_prices = {}
                    for ts_code in target_stocks:
                        cur = conn.execute("""
                            SELECT close FROM daily_a 
                            WHERE ts_code = ? AND trade_date <= ?
                            ORDER BY trade_date DESC LIMIT 1
                        """, (ts_code, trade_date))
                        row = cur.fetchone()
                        if row:
                            current_prices[ts_code] = row[0]
                    
                    # 卖出不在目标中的持仓
                    for ts_code in list(positions.keys()):
                        if ts_code not in target_stocks and positions.get(ts_code, 0) > 0:
                            price = current_prices.get(ts_code)
                            if price:
                                # 卖出
                                sell_value = positions[ts_code] * price * 0.998  # 假设千二手续费
                                cash += sell_value
                                rebalance_log.append({
                                    'date': trade_date,
                                    'action': 'sell',
                                    'symbol': ts_code,
                                    'price': price,
                                    'shares': positions[ts_code],
                                    'value': sell_value
                                })
                                del positions[ts_code]
                    
                    # 买入新目标
                    if cash > 0 and target_stocks:
                        per_stock_cash = cash / len(target_stocks)
                        for ts_code in target_stocks:
                            price = current_prices.get(ts_code)
                            if price and price > 0:
                                shares = int(per_stock_cash / price / 100) * 100  # 取整百股
                                if shares > 0:
                                    cost = shares * price * 1.002  # 买入手续费
                                    if cost <= cash:
                                        positions[ts_code] = positions.get(ts_code, 0) + shares
                                        cash -= cost
                                        rebalance_log.append({
                                            'date': trade_date,
                                            'action': 'buy',
                                            'symbol': ts_code,
                                            'price': price,
                                            'shares': shares,
                                            'value': cost
                                        })
                    
                    self._current_positions = target_stocks
                
                self._last_rebalance_date = trade_date
            
            # 计算当前权益
            total_value = cash
            for ts_code, shares in positions.items():
                cur = conn.execute("""
                    SELECT close FROM daily_a 
                    WHERE ts_code = ? AND trade_date <= ?
                    ORDER BY trade_date DESC LIMIT 1
                """, (ts_code, trade_date))
                row = cur.fetchone()
                if row:
                    total_value += shares * row[0]
            
            equity_curve.append({
                'trade_date': trade_date,
                'equity': total_value,
                'cash': cash,
                'positions': len(positions)
            })
        
        conn.close()
        
        # 计算收益率
        initial_equity = self.initial_cash
        final_equity = equity_curve[-1]['equity'] if equity_curve else initial_equity
        total_return = (final_equity - initial_equity) / initial_equity * 100
        
        # 计算年化收益率
        years = len(trading_days) / 252
        annual_return = ((final_equity / initial_equity) ** (1 / years) - 1) * 100 if years > 0 else 0
        
        # 计算最大回撤
        peak = initial_equity
        max_dd = 0
        dd_peak_date = trading_days[0] if trading_days else ''
        dd_trough_date = trading_days[0]
        
        for e in equity_curve:
            if e['equity'] > peak:
                peak = e['equity']
            dd = (peak - e['equity']) / peak * 100
            if dd > max_dd:
                max_dd = dd
                dd_peak_date = e['trade_date']
                dd_trough_date = e['trade_date']
        
        # 打印结果
        print(f"\n=== Backtest Results ===")
        print(f"Initial Equity: {initial_equity:,.0f}")
        print(f"Final Equity: {final_equity:,.0f}")
        print(f"Total Return: {total_return:.2f}%")
        print(f"Annual Return: {annual_return:.2f}%")
        print(f"Max Drawdown: {max_dd:.2f}%")
        print(f"Peak: {dd_peak_date}, Trough: {dd_trough_date}")
        
        return {
            'initial_equity': initial_equity,
            'final_equity': final_equity,
            'total_return': total_return,
            'annual_return': annual_return,
            'max_drawdown': max_dd,
            'equity_curve': equity_curve,
            'rebalance_log': rebalance_log,
        }


def run_strategy():
    """运行策略回测"""
    strategy = IgnoredStockStrategy(
        db_path_raw='/workspace/quant_strategy_v2/data/data.sqlite',
        start_date='20160301',
        end_date='20260301',
        rebalance_months=3,
        top_k=5,
        min_market_cap=100.0,  # 100亿
        max_price_range=5.0,
        initial_cash=1_000_000.0,
    )
    
    result = strategy.run_backtest()
    
    # 保存结果
    import json
    with open('/workspace/openclaw-output/ignored_stock_result.json', 'w', encoding='utf-8') as f:
        json.dump({
            'initial_equity': result['initial_equity'],
            'final_equity': result['final_equity'],
            'total_return': result['total_return'],
            'annual_return': result['annual_return'],
            'max_drawdown': result['max_drawdown'],
        }, f, indent=2, ensure_ascii=False)
    
    # 保存权益曲线
    import csv
    with open('/workspace/openclaw-output/ignored_stock_equity.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['trade_date', 'equity', 'cash', 'positions'])
        writer.writeheader()
        for e in result['equity_curve']:
            writer.writerow(e)
    
    print("\nResults saved to /workspace/openclaw-output/")
    
    return result


if __name__ == "__main__":
    run_strategy()
