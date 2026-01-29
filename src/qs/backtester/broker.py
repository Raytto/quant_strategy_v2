from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Dict


@dataclass
class Position:
    size: float = 0.0
    avg_price: float = 0.0

    @property
    def market_value(self) -> float:
        return self.size * self.avg_price


@dataclass
class TradeRecord:
    trade_date: str
    action: str  # BUY / SELL / WRITE_OFF
    symbol: str  # 交易标的代码
    price: float  # 原始信号价(开盘价) / 或写入 0 (WRITE_OFF)
    exec_price: float  # 含滑点执行价 / 0 (WRITE_OFF)
    size: float
    gross_amount: float  # 买入=成交金额  卖出=成交金额 / 0
    fees: float  # 佣金(+卖出印花税) / 0
    cash_after: float
    position_after: float
    equity_after: float


# --- Simplified models (Commission / Slippage) ---------------------------------
@dataclass
class CommissionInfo:
    commission_rate: float = 0.00015  # 0.015%
    tax_rate: float = 0.0005  # 0.05% (sell side)
    min_commission: float = 5.0  # minimum per trade

    def buy_fees(self, gross_amount: float) -> float:
        # 仅佣金
        return max(gross_amount * self.commission_rate, self.min_commission)

    def sell_fees(self, gross_amount: float) -> float:
        commission = max(gross_amount * self.commission_rate, self.min_commission)
        tax = gross_amount * self.tax_rate
        return commission + tax


@dataclass
class SlippageModel:
    slippage: float = 0.0002  # 0.02%

    def adjust_price(self, price: float, side: str) -> float:
        if side == "BUY":
            return price * (1 + self.slippage)
        else:
            return price * (1 - self.slippage)


class Broker:
    """Prototype multi-symbol broker.

    Backward compatible single-symbol API kept (symbol & position attributes) while
    introducing positions dict + last_prices for portfolio valuation.
    New symbol-specific helpers: buy_sym / sell_sym / buy_all_sym / sell_all_sym.
    """

    def __init__(
        self,
        cash: float,
        enable_trade_log: bool = False,
        commission_rate: float = 0.00015,
        tax_rate: float = 0.0005,
        slippage: float = 0.0002,
        min_commission: float = 5.0,
        symbol: str = "",
    ):
        self.cash = cash
        # legacy single position references default symbol; real storage in positions dict
        self.symbol = symbol
        self.positions: Dict[str, Position] = {}
        if symbol:
            self.positions[symbol] = Position()
        self.enable_trade_log = enable_trade_log
        self.trades: List[TradeRecord] = []
        self.commission_rate = commission_rate
        self.tax_rate = tax_rate
        self.slippage = slippage
        self.min_commission = min_commission
        self.total_fees: float = 0.0
        self._commission = CommissionInfo(commission_rate, tax_rate, min_commission)
        self._slippage_model = SlippageModel(slippage)
        # last mark prices per symbol for equity calc
        self.last_prices: Dict[str, float] = {}

    # -------------------- position helpers -------------------------------------
    def _get_position(self, symbol: str) -> Position:
        pos = self.positions.get(symbol)
        if pos is None:
            pos = Position()
            self.positions[symbol] = pos
        return pos

    # backward compatibility property (default symbol only)
    @property
    def position(self) -> Position:
        return self._get_position(self.symbol) if self.symbol else Position()

    # -------------------- equity / marks ---------------------------------------
    def update_marks(self, price_map: Dict[str, float]):
        self.last_prices.update(price_map)

    def total_equity(self, fallback_price: float | None = None) -> float:
        equity = self.cash
        for sym, pos in self.positions.items():
            price = self.last_prices.get(sym)
            if price is None:
                # use avg_price or provided fallback
                price = fallback_price if fallback_price is not None else pos.avg_price
            equity += pos.size * price
        return equity

    # ------------------------------------------------------------------
    def _log_trade(self, record: TradeRecord):
        self.trades.append(record)
        self.total_fees += record.fees
        if self.enable_trade_log:
            print(
                f"TRADE {record.trade_date} {record.action} {record.symbol} px={record.price:.2f} exec={record.exec_price:.4f} "
                f"size={record.size:.0f} gross={record.gross_amount:.2f} fees={record.fees:.2f} "
                f"cash={record.cash_after:.2f} pos={record.position_after:.0f} eq={record.equity_after:.2f}"
            )

    # -------------------- 强制核销 (退市/清零) ---------------------------------
    def force_write_off(self, trade_date: str, symbol: str, reason: str = "delist"):
        """将持仓按 0 价值核销，不收取费用，记录一条 WRITE_OFF 交易。"""
        pos = self.positions.get(symbol)
        if not pos or pos.size <= 0:
            return 0
        size = int(pos.size)
        # 价值归零
        self.last_prices[symbol] = 0.0
        pos.size = 0
        pos.avg_price = 0.0
        eq = self.total_equity()
        rec = TradeRecord(
            trade_date,
            "WRITE_OFF",
            symbol,
            0.0,
            0.0,
            size,
            0.0,
            0.0,
            self.cash,  # 现金不变
            0,
            eq,
        )
        self._log_trade(rec)
        if self.enable_trade_log:
            print(f"WRITE_OFF {symbol} at {trade_date} reason={reason}")
        return size

    # ------------------------------------------------------------------
    # Internal execution helpers (symbol-aware)
    def _execute_buy(self, trade_date: str, symbol: str, price: float, size: int):
        if size <= 0 or price <= 0:
            return 0
        exec_price = self._slippage_model.adjust_price(price, "BUY")
        gross_cost = exec_price * size
        fees = self._commission.buy_fees(gross_cost)
        total_out = gross_cost + fees
        if total_out > self.cash:
            while size > 0:
                gross_cost = exec_price * size
                fees = self._commission.buy_fees(gross_cost)
                total_out = gross_cost + fees
                if total_out <= self.cash:
                    break
                size -= 1
            if size <= 0:
                return 0
        gross_cost = exec_price * size
        fees = self._commission.buy_fees(gross_cost)
        self.cash -= gross_cost + fees
        pos = self._get_position(symbol)
        prev_value = pos.avg_price * pos.size
        new_value = prev_value + gross_cost
        pos.size += size
        pos.avg_price = new_value / pos.size if pos.size > 0 else 0.0
        self.last_prices[symbol] = exec_price
        eq = self.total_equity()
        self._log_trade(
            TradeRecord(
                trade_date,
                "BUY",
                symbol,
                price,
                exec_price,
                size,
                gross_cost,
                fees,
                self.cash,
                pos.size,
                eq,
            )
        )
        return size

    def _execute_sell(self, trade_date: str, symbol: str, price: float, size: int):
        if size <= 0 or price <= 0:
            return 0
        pos = self._get_position(symbol)
        if pos.size <= 0:
            return 0
        if size > pos.size:
            size = int(pos.size)
        exec_price = self._slippage_model.adjust_price(price, "SELL")
        gross_proceeds = exec_price * size
        fees = self._commission.sell_fees(gross_proceeds)
        net_in = gross_proceeds - fees
        self.cash += net_in
        pos.size -= size
        if pos.size == 0:
            pos.avg_price = 0.0
        self.last_prices[symbol] = exec_price
        eq = self.total_equity()
        self._log_trade(
            TradeRecord(
                trade_date,
                "SELL",
                symbol,
                price,
                exec_price,
                size,
                gross_proceeds,
                fees,
                self.cash,
                pos.size,
                eq,
            )
        )
        return size

    # ------------------------------------------------------------------
    # New multi-symbol public API
    def buy_sym(
        self, trade_date: str, symbol: str, price: float, size: Optional[int] = None
    ):
        if size is None:
            exec_price = self._slippage_model.adjust_price(price, "BUY")
            est_per_share = exec_price * (1 + self.commission_rate)
            size = int(self.cash // est_per_share)
        return self._execute_buy(trade_date, symbol, price, int(size))

    def sell_sym(
        self, trade_date: str, symbol: str, price: float, size: Optional[int] = None
    ):
        pos = self._get_position(symbol)
        if size is None:
            size = int(pos.size)
        return self._execute_sell(trade_date, symbol, price, int(size))

    def buy_all_sym(self, trade_date: str, symbol: str, price: float):
        return self.buy_sym(trade_date, symbol, price, None)

    def sell_all_sym(self, trade_date: str, symbol: str, price: float):
        return self.sell_sym(trade_date, symbol, price, None)

    def order_target_percent_sym(
        self, trade_date: str, symbol: str, price: float, target: float
    ):
        target = max(0.0, min(1.0, target))
        # Ensure the current symbol is marked at the executable reference price (typically today's open)
        self.last_prices[symbol] = price
        equity = self.total_equity()
        exec_price = self._slippage_model.adjust_price(
            price, "BUY" if target > 0 else "SELL"
        )
        target_value = equity * target
        target_size = int(target_value // exec_price)
        pos = self._get_position(symbol)
        delta = target_size - int(pos.size)
        if delta > 0:
            return self._execute_buy(trade_date, symbol, price, delta)
        elif delta < 0:
            return self._execute_sell(trade_date, symbol, price, -delta)
        return 0

    def rebalance_target_percents(
        self,
        trade_date: str,
        price_map: Dict[str, float],
        target_weights: Dict[str, float],
    ):
        """Batch rebalance to target percentage weights (similar to backtrader style).

        Steps:
          1. Clamp weights into [0,1]; compute union of existing + target symbols.
          2. Compute current equity using existing marks (or provided prices for new symbols).
          3. Determine target sizes (floor int) using execution price with slippage.
          4. Execute ALL sells first (including targets=0) then buys to free cash.
        Notes:
          - Rounding causes residual cash.
          - price_map 应包含待买/卖标的开盘价 (执行价基价)。
        """
        # Normalize / clamp
        clean_weights: Dict[str, float] = {}
        for sym, w in target_weights.items():
            if w <= 0:
                continue
            clean_weights[sym] = min(1.0, max(0.0, w))
        # union of symbols
        symbols = set(self.positions.keys()) | set(clean_weights.keys())
        # Mark provided prices (typically today's open) so equity/targets are based on current valuations
        for sym, p in price_map.items():
            if p > 0:
                self.last_prices[sym] = p
        equity = self.total_equity()
        # Build target sizes
        sells: List[tuple[str, int, float]] = []  # (sym, delta_size, raw_price)
        buys: List[tuple[str, int, float]] = []
        for sym in symbols:
            target_w = clean_weights.get(sym, 0.0)
            price = price_map.get(sym)
            if price is None or price <= 0:
                continue  # skip if no executable price
            exec_price = self._slippage_model.adjust_price(
                price, "BUY" if target_w > 0 else "SELL"
            )
            target_value = equity * target_w
            target_size = int(target_value // exec_price)
            pos = self._get_position(sym)
            delta = target_size - int(pos.size)
            if delta < 0:  # need to sell
                sells.append((sym, -delta, price))
            elif delta > 0:
                buys.append((sym, delta, price))
        # Execute sells first
        for sym, sz, px in sells:
            self._execute_sell(trade_date, sym, px, sz)
        # Then buys
        for sym, sz, px in buys:
            if sz > 0:
                self._execute_buy(trade_date, sym, px, sz)
        return {"sells": len(sells), "buys": len(buys)}

    # ------------------------------------------------------------------
    # Backward compatible single-symbol API (assumes default self.symbol)
    def buy(self, trade_date: str, price: float, size: Optional[int] = None):
        if not self.symbol:
            return 0
        return self.buy_sym(trade_date, self.symbol, price, size)

    def sell(self, trade_date: str, price: float, size: Optional[int] = None):
        if not self.symbol:
            return 0
        return self.sell_sym(trade_date, self.symbol, price, size)

    def close(self, trade_date: str, price: float):
        return self.sell(trade_date, price, None)

    def order_target_size(self, trade_date: str, price: float, size: int):
        if not self.symbol:
            return 0
        pos = self._get_position(self.symbol)
        delta = size - int(pos.size)
        if delta > 0:
            return self._execute_buy(trade_date, self.symbol, price, delta)
        elif delta < 0:
            return self._execute_sell(trade_date, self.symbol, price, -delta)
        return 0

    def order_target_percent(self, trade_date: str, price: float, target: float):
        if not self.symbol:
            return 0
        return self.order_target_percent_sym(trade_date, self.symbol, price, target)

    def order_target_value(self, trade_date: str, price: float, target_value: float):
        if target_value < 0:
            target_value = 0
        if not self.symbol:
            return 0
        exec_price = self._slippage_model.adjust_price(
            price, "BUY" if target_value > 0 else "SELL"
        )
        target_size = int(target_value // exec_price)
        return self.order_target_size(trade_date, price, target_size)

    # legacy helpers
    def buy_all(self, trade_date: str, price: float):
        return self.buy(trade_date, price, None)

    def sell_all(self, trade_date: str, price: float):
        return self.close(trade_date, price)

    # Multi-symbol explicit helpers naming consistency
    def set_default_symbol(self, symbol: str):
        self.symbol = symbol
        self._get_position(symbol)
