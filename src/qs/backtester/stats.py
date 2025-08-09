from __future__ import annotations
from math import sqrt
from collections import OrderedDict
from typing import Iterable, Dict, Tuple, List, Any

# The functions below operate on an iterable of objects each having
# attributes: trade_date (YYYYMMDD string) and equity (float).


def compute_annual_returns(curve: Iterable[Any]) -> OrderedDict:
    curve_list = list(curve)
    if not curve_list:
        return OrderedDict()
    year_start = {}
    year_end = {}
    for pt in curve_list:
        y = pt.trade_date[:4]
        if y not in year_start:
            year_start[y] = pt.equity
        year_end[y] = pt.equity
    years = sorted(year_end.keys())
    ann = OrderedDict()
    prev_year_end = None
    for i, y in enumerate(years):
        if i == 0:
            start_eq = year_start[y]
        else:
            start_eq = prev_year_end
        end_eq = year_end[y]
        ann[y] = end_eq / start_eq - 1.0 if start_eq > 0 else 0.0
        prev_year_end = end_eq
    return ann


def compute_max_drawdown(curve: Iterable[Any]) -> Tuple[float, str | None, str | None]:
    curve_list = list(curve)
    if not curve_list:
        return 0.0, None, None
    peak_eq = curve_list[0].equity
    peak_date = curve_list[0].trade_date
    max_dd = 0.0  # negative value
    max_dd_peak = peak_date
    max_dd_trough = peak_date
    for pt in curve_list[1:]:
        if pt.equity > peak_eq:
            peak_eq = pt.equity
            peak_date = pt.trade_date
        dd = pt.equity / peak_eq - 1.0
        if dd < max_dd:
            max_dd = dd
            max_dd_peak = peak_date
            max_dd_trough = pt.trade_date
    return max_dd, max_dd_peak, max_dd_trough


def compute_daily_returns(curve: Iterable[Any]) -> List[float]:
    curve_list = list(curve)
    rets: List[float] = []
    for i in range(1, len(curve_list)):
        prev = curve_list[i - 1].equity
        curr = curve_list[i].equity
        if prev > 0:
            rets.append(curr / prev - 1.0)
    return rets


def compute_risk_metrics(
    curve: Iterable[Any], initial_equity: float, ann_factor: int = 252
) -> Dict[str, float]:
    curve_list = list(curve)
    if not curve_list:
        return {}
    daily_rets = compute_daily_returns(curve_list)
    if not daily_rets:
        return {}
    import statistics

    total_days = len(daily_rets)
    final_equity = curve_list[-1].equity
    cagr = (
        (final_equity / initial_equity) ** (ann_factor / total_days) - 1
        if total_days > 0
        else 0.0
    )
    mean_daily = statistics.fmean(daily_rets)
    std_daily = statistics.pstdev(daily_rets) if len(daily_rets) > 1 else 0.0
    ann_vol = std_daily * sqrt(ann_factor)
    ann_return = (1 + mean_daily) ** ann_factor - 1
    sharpe = ann_return / ann_vol if ann_vol > 0 else 0.0  # rf assumed 0
    win_rate = sum(1 for r in daily_rets if r > 0) / len(daily_rets)
    return {
        "CAGR": cagr,
        "AnnReturn": ann_return,
        "AnnVol": ann_vol,
        "Sharpe": sharpe,
        "WinRate": win_rate,
    }


__all__ = [
    "compute_annual_returns",
    "compute_max_drawdown",
    "compute_daily_returns",
    "compute_risk_metrics",
]
