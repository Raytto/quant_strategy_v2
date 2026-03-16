from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from ..backtester.data import Bar
from ..backtester.market import StrategyContext


@dataclass(frozen=True)
class IgnoredCrowdedAHConfig:
    buy_price_pos_max: float = 0.20
    buy_flat_max: float = 0.16
    buy_liq_pct_max: float = 0.40
    sell_price_pos_min: float = 0.75
    sell_liq_pct_min: float = 0.90
    max_positions: int = 5
    max_pe: float = 40.0
    min_market_cap_yiyuan: float = 100.0


FINAL_CONFIG = IgnoredCrowdedAHConfig()


def buy_mask(df: pd.DataFrame, config: IgnoredCrowdedAHConfig = FINAL_CONFIG) -> pd.Series:
    pe_ok = (df["pe_proxy"] > 0) & (df["pe_proxy"] <= config.max_pe)
    mcap_ok = df["market_cap_proxy"] >= config.min_market_cap_yiyuan
    return (
        pe_ok
        & mcap_ok
        & (df["price_pos_36m"] <= config.buy_price_pos_max)
        & (df["flat_3m"] <= config.buy_flat_max)
        & (df["amt_pct_3m"] <= config.buy_liq_pct_max)
        & (df["vol_pct_3m"] <= config.buy_liq_pct_max)
    )


def sell_mask(df: pd.DataFrame, config: IgnoredCrowdedAHConfig = FINAL_CONFIG) -> pd.Series:
    pe_ok = (df["pe_proxy"] > 0) & (df["pe_proxy"] <= config.max_pe)
    mcap_ok = df["market_cap_proxy"] >= config.min_market_cap_yiyuan
    return (
        pe_ok
        & mcap_ok
        & (df["price_pos_36m"] >= config.sell_price_pos_min)
        & (
            (df["amt_pct_3m"] >= config.sell_liq_pct_min)
            | (df["vol_pct_3m"] >= config.sell_liq_pct_min)
        )
    )


def top_ignored(
    df: pd.DataFrame,
    config: IgnoredCrowdedAHConfig = FINAL_CONFIG,
    *,
    limit: int | None = None,
) -> pd.DataFrame:
    limit = config.max_positions if limit is None else int(limit)
    candidates = df.loc[buy_mask(df, config)].copy()
    return candidates.sort_values(
        ["ignored_score", "market_cap_proxy"],
        ascending=[False, False],
    ).head(limit)


def load_trade_panel(panel_path: str | Path) -> pd.DataFrame:
    panel = pd.read_pickle(Path(panel_path))
    panel = panel.copy()
    panel["ym"] = panel["ym"].astype(str)
    panel["trade_date"] = panel["trade_date"].astype(str)
    return panel


def build_month_maps(panel: pd.DataFrame) -> tuple[list[str], dict[str, pd.DataFrame]]:
    use_cols = [
        "ts_code",
        "ym",
        "trade_date",
        "open_adj",
        "close_adj",
        "ignored_score",
        "price_pos_36m",
        "flat_3m",
        "amt_pct_3m",
        "vol_pct_3m",
        "pe_proxy",
        "market_cap_proxy",
        "market",
        "name",
    ]
    month_maps: dict[str, pd.DataFrame] = {}
    months = sorted(panel["ym"].dropna().unique().tolist())
    for ym, df in panel[use_cols].groupby("ym", sort=False):
        month_maps[str(ym)] = df.set_index("ts_code").sort_values(
            ["ignored_score", "market_cap_proxy"],
            ascending=[False, False],
        )
    return months, month_maps


def build_monthly_bars_from_panel(
    panel: pd.DataFrame,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[Bar]:
    cal = (
        panel.groupby("ym", as_index=False)["trade_date"]
        .max()
        .sort_values("trade_date")
        .reset_index(drop=True)
    )
    if start_date:
        cal = cal.loc[cal["trade_date"] >= str(start_date)]
    if end_date:
        cal = cal.loc[cal["trade_date"] <= str(end_date)]
    bars: list[Bar] = []
    for row in cal.itertuples(index=False):
        bars.append(
            Bar(
                trade_date=str(row.trade_date),
                open=1.0,
                high=1.0,
                low=1.0,
                close=1.0,
                pct_chg=None,
            )
        )
    return bars


class IgnoredCrowdedAHMonthlyStrategy:
    def __init__(
        self,
        *,
        panel_path: str | Path = "data/backtests/ignored_buzz_ah/cache/trade_panel.pkl",
        start_date: str = "20170731",
        config: IgnoredCrowdedAHConfig = FINAL_CONFIG,
    ):
        self.panel_path = Path(panel_path)
        self.start_date = str(start_date)
        self.config = config

        self.panel = load_trade_panel(self.panel_path)
        self.months, self.month_maps = build_month_maps(self.panel)
        self.trade_date_to_ym = {
            str(v): str(k)
            for k, v in (
                self.panel.groupby("ym", as_index=False)["trade_date"].max().itertuples(index=False)
            )
        }

        self.rebalance_history: list[dict[str, Any]] = []
        self.state_history: list[dict[str, Any]] = []

    def _ym_for_trade_date(self, trade_date: str) -> str:
        return self.trade_date_to_ym.get(str(trade_date), str(trade_date)[:6])

    @staticmethod
    def _tradable_hold_df(df: pd.DataFrame) -> pd.DataFrame:
        return df.loc[
            df["open_adj"].notna()
            & df["close_adj"].notna()
            & (df["open_adj"] > 0)
            & (df["close_adj"] > 0)
        ].copy()

    def _record_state(self, trade_date: str, positions: dict[str, float]) -> None:
        active = sorted(sym for sym, size in positions.items() if size > 0)
        self.state_history.append(
            {
                "trade_date": trade_date,
                "ym": self._ym_for_trade_date(trade_date),
                "position_count": len(active),
                "holdings": ",".join(active),
            }
        )

    def on_start(self, feed, broker) -> None:  # noqa: ANN001
        self.rebalance_history = []
        self.state_history = []

    def on_bar_ctx(self, ctx: StrategyContext) -> None:
        current_positions = dict(ctx.portfolio.positions)
        if ctx.signal_date is None or ctx.trade_date < self.start_date:
            self._record_state(ctx.trade_date, current_positions)
            return

        signal_ym = self._ym_for_trade_date(ctx.signal_date)
        hold_ym = self._ym_for_trade_date(ctx.trade_date)
        sig_df = self.month_maps.get(signal_ym)
        hold_df_raw = self.month_maps.get(hold_ym)
        if sig_df is None or hold_df_raw is None:
            self._record_state(ctx.trade_date, current_positions)
            return

        hold_df = self._tradable_hold_df(hold_df_raw)
        if hold_df.empty:
            self._record_state(ctx.trade_date, current_positions)
            return

        buy_ok = buy_mask(sig_df, self.config)
        sell_ok = sell_mask(sig_df, self.config)

        current_holdings = sorted(current_positions.keys())
        retained: list[str] = []
        sold: list[str] = []
        for sym in current_holdings:
            if sym not in sig_df.index or sym not in hold_df.index:
                sold.append(sym)
                continue
            if bool(sell_ok.loc[sym]):
                sold.append(sym)
                continue
            retained.append(sym)

        slots = max(0, self.config.max_positions - len(retained))
        available = sig_df.loc[
            buy_ok & (~sig_df.index.isin(retained)) & sig_df.index.isin(hold_df.index)
        ]
        buys = available.head(slots).index.tolist()
        target_symbols = retained + buys

        trade_symbols = set(target_symbols) | set(current_holdings)
        if trade_symbols:
            price_map = {}
            for sym in trade_symbols:
                if sym in hold_df.index:
                    price_map[sym] = float(hold_df.at[sym, "open_adj"])
                    continue
                if sym in sig_df.index and pd.notna(sig_df.at[sym, "close_adj"]):
                    fallback_px = float(sig_df.at[sym, "close_adj"])
                    if fallback_px > 0:
                        price_map[sym] = fallback_px
                        continue
            target_weights = {
                sym: 1.0 / len(target_symbols)
                for sym in target_symbols
            } if target_symbols else {}
            mark_map = {
                sym: float(hold_df.at[sym, "close_adj"])
                for sym in current_holdings
                if sym in hold_df.index
                and pd.notna(hold_df.at[sym, "close_adj"])
                and float(hold_df.at[sym, "close_adj"]) > 0
            }
            if mark_map:
                ctx.set_mark_request(prices=mark_map)
            ctx.rebalance_to_weights(
                target_weights,
                execution_prices=price_map,
            )
        else:
            hold_marks = {
                sym: float(hold_df.at[sym, "close_adj"])
                for sym in current_holdings
                if sym in hold_df.index
                and pd.notna(hold_df.at[sym, "close_adj"])
                and float(hold_df.at[sym, "close_adj"]) > 0
            }
            if hold_marks:
                ctx.set_mark_request(prices=hold_marks)

        self.rebalance_history.append(
            {
                "signal_month": signal_ym,
                "hold_month": hold_ym,
                "sold": ",".join(sold),
                "retained": ",".join(retained),
                "bought": ",".join(buys),
                "target": ",".join(target_symbols),
                "position_count": len(target_symbols),
            }
        )
        next_positions = {sym: 1.0 for sym in target_symbols} if trade_symbols else current_positions
        self._record_state(ctx.trade_date, next_positions)


__all__ = [
    "IgnoredCrowdedAHConfig",
    "FINAL_CONFIG",
    "buy_mask",
    "sell_mask",
    "top_ignored",
    "load_trade_panel",
    "build_month_maps",
    "build_monthly_bars_from_panel",
    "IgnoredCrowdedAHMonthlyStrategy",
]
