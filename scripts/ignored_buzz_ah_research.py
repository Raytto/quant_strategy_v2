from __future__ import annotations

"""Research script for a "buy when ignored, sell when crowded" A/H strategy.

The local data has an important limitation:
  - A-share PE history is only available from `bak_daily_a`, which starts in 2017-06.
  - H-share native PE is not present. For dual-listed A/H names we approximate H PE
    from the paired A-share PE and the A/H price ratio.

The script therefore:
  1. builds monthly panels for A shares and H shares,
  2. derives "ignored" / "crowded" signals,
  3. grid-searches a small parameter set,
  4. exports the best run and benchmark comparison.
"""

import argparse
import csv
import json
import math
import sqlite3
from dataclasses import asdict, dataclass
from itertools import product
from pathlib import Path
from typing import Any, Iterable

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import _bootstrap  # noqa: F401
from qs.backtester.stats import compute_max_drawdown, compute_risk_metrics


OUTPUT_DIR = Path("data/backtests/ignored_buzz_ah")
DB_PATH = Path("data/data.sqlite")
PAIRS_CSV = Path("data/ah_codes.csv")

MIN_MARKET_CAP_YIYUAN = 100.0
MAX_PE = 40.0
MAX_POSITIONS = 5


@dataclass(frozen=True)
class StrategyParams:
    buy_price_pos_max: float
    buy_flat_max: float
    buy_liq_pct_max: float
    sell_price_pos_min: float
    sell_liq_pct_min: float
    max_positions: int = MAX_POSITIONS
    max_pe: float = MAX_PE
    min_market_cap_yiyuan: float = MIN_MARKET_CAP_YIYUAN


@dataclass(frozen=True)
class CurvePoint:
    trade_date: str
    equity: float


def load_pairs(path: Path) -> pd.DataFrame:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise RuntimeError(f"empty pairs file: {path}")
        hk_col = "hk_code" if "hk_code" in reader.fieldnames else "c"
        rows: list[dict[str, str]] = []
        for row in reader:
            cn_code = (row.get("cn_code") or "").strip()
            hk_code = (row.get(hk_col) or "").strip()
            if not cn_code or not hk_code:
                continue
            rows.append(
                {
                    "pair_name": (row.get("name") or "").strip(),
                    "cn_code": cn_code,
                    "hk_code": hk_code,
                }
            )
    return pd.DataFrame(rows)


def _latest_adj_table(con: sqlite3.Connection, table: str) -> pd.DataFrame:
    query = f"""
    SELECT a.ts_code, a.adj_factor AS base_adj
    FROM {table} a
    JOIN (
      SELECT ts_code, MAX(trade_date) AS last_date
      FROM {table}
      GROUP BY ts_code
    ) t
      ON a.ts_code=t.ts_code AND a.trade_date=t.last_date
    """
    return pd.read_sql_query(query, con)


def load_fx_rates(con: sqlite3.Connection) -> pd.DataFrame:
    query = """
    SELECT trade_date, ts_code, bid_close, ask_close
    FROM fx_daily
    WHERE ts_code IN ('USDCNH.FXCM','USDHKD.FXCM')
    ORDER BY trade_date
    """
    fx = pd.read_sql_query(query, con)
    fx["trade_date"] = pd.to_datetime(fx["trade_date"], format="%Y%m%d")
    fx["mid"] = (fx["bid_close"] + fx["ask_close"]) / 2.0
    fx = fx.pivot(index="trade_date", columns="ts_code", values="mid").sort_index()
    fx["hk_to_cny"] = fx["USDCNH.FXCM"] / fx["USDHKD.FXCM"]
    out = fx[["hk_to_cny"]].dropna().reset_index()
    return out


def build_monthly_a(con: sqlite3.Connection, cache_path: Path, refresh: bool) -> pd.DataFrame:
    if cache_path.exists() and not refresh:
        return pd.read_pickle(cache_path)

    query = """
    WITH month_agg AS (
      SELECT ts_code,
             substr(trade_date,1,6) AS ym,
             MIN(trade_date) AS month_start,
             MAX(trade_date) AS month_end,
             SUM(amount) AS month_amount,
             SUM(vol) AS month_vol
      FROM daily_a
      WHERE trade_date >= '20120101'
      GROUP BY ts_code, substr(trade_date,1,6)
    )
    SELECT m.ts_code,
           m.ym,
           m.month_start,
           m.month_end,
           ds.open AS open_raw,
           de.close AS close_raw,
           m.month_amount,
           m.month_vol,
           COALESCE(afs.adj_factor, 1.0) AS open_adj_factor,
           COALESCE(afe.adj_factor, 1.0) AS close_adj_factor,
           b.pe,
           b.total_mv,
           s.name
    FROM month_agg m
    JOIN daily_a ds
      ON ds.ts_code=m.ts_code AND ds.trade_date=m.month_start
    JOIN daily_a de
      ON de.ts_code=m.ts_code AND de.trade_date=m.month_end
    LEFT JOIN adj_factor_a afs
      ON afs.ts_code=m.ts_code AND afs.trade_date=m.month_start
    LEFT JOIN adj_factor_a afe
      ON afe.ts_code=m.ts_code AND afe.trade_date=m.month_end
    LEFT JOIN bak_daily_a b
      ON b.ts_code=m.ts_code AND b.trade_date=m.month_end
    LEFT JOIN stock_basic_a s
      ON s.ts_code=m.ts_code
    ORDER BY m.ts_code, m.ym
    """
    df = pd.read_sql_query(query, con)
    latest_adj = _latest_adj_table(con, "adj_factor_a")
    df = df.merge(latest_adj, on="ts_code", how="left")
    df["base_adj"] = df["base_adj"].fillna(1.0)
    df["open_adj"] = df["open_raw"] * df["open_adj_factor"] / df["base_adj"]
    df["close_adj"] = df["close_raw"] * df["close_adj_factor"] / df["base_adj"]
    df["month_start"] = pd.to_datetime(df["month_start"], format="%Y%m%d")
    df["month_end"] = pd.to_datetime(df["month_end"], format="%Y%m%d")
    df["market"] = "A"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_pickle(cache_path)
    return df


def build_monthly_h(
    con: sqlite3.Connection,
    cache_path: Path,
    refresh: bool,
    fx_rates: pd.DataFrame,
    pairs: pd.DataFrame,
    a_monthly: pd.DataFrame,
) -> pd.DataFrame:
    if cache_path.exists() and not refresh:
        return pd.read_pickle(cache_path)

    query = """
    WITH month_agg AS (
      SELECT ts_code,
             substr(trade_date,1,6) AS ym,
             MIN(trade_date) AS month_start,
             MAX(trade_date) AS month_end,
             SUM(amount) AS month_amount,
             SUM(vol) AS month_vol
      FROM daily_h
      WHERE trade_date >= '20120101'
      GROUP BY ts_code, substr(trade_date,1,6)
    )
    SELECT m.ts_code,
           m.ym,
           m.month_start,
           m.month_end,
           ds.open AS open_raw,
           de.close AS close_raw,
           m.month_amount,
           m.month_vol,
           COALESCE(afs.adj_factor, 1.0) AS open_adj_factor,
           COALESCE(afe.adj_factor, 1.0) AS close_adj_factor,
           s.name
    FROM month_agg m
    JOIN daily_h ds
      ON ds.ts_code=m.ts_code AND ds.trade_date=m.month_start
    JOIN daily_h de
      ON de.ts_code=m.ts_code AND de.trade_date=m.month_end
    LEFT JOIN adj_factor_h afs
      ON afs.ts_code=m.ts_code AND afs.trade_date=m.month_start
    LEFT JOIN adj_factor_h afe
      ON afe.ts_code=m.ts_code AND afe.trade_date=m.month_end
    LEFT JOIN stock_basic_h s
      ON s.ts_code=m.ts_code
    ORDER BY m.ts_code, m.ym
    """
    df = pd.read_sql_query(query, con)
    latest_adj = _latest_adj_table(con, "adj_factor_h")
    df = df.merge(latest_adj, on="ts_code", how="left")
    df["base_adj"] = df["base_adj"].fillna(1.0)
    df["month_start"] = pd.to_datetime(df["month_start"], format="%Y%m%d")
    df["month_end"] = pd.to_datetime(df["month_end"], format="%Y%m%d")

    fx_rates = fx_rates.sort_values("trade_date")
    start_fx = pd.merge_asof(
        df.sort_values("month_start"),
        fx_rates.rename(columns={"trade_date": "month_start", "hk_to_cny": "fx_open"}),
        on="month_start",
        direction="backward",
    )
    end_fx = pd.merge_asof(
        start_fx.sort_values("month_end"),
        fx_rates.rename(columns={"trade_date": "month_end", "hk_to_cny": "fx_close"}),
        on="month_end",
        direction="backward",
    )

    df = end_fx.merge(pairs, left_on="ts_code", right_on="hk_code", how="inner")
    a_ref = a_monthly[
        [
            "ts_code",
            "ym",
            "close_raw",
            "pe",
            "total_mv",
            "name",
        ]
    ].rename(
        columns={
            "ts_code": "cn_code",
            "close_raw": "a_close_raw",
            "pe": "a_pe",
            "total_mv": "a_total_mv",
            "name": "a_name",
        }
    )
    df = df.merge(a_ref, on=["cn_code", "ym"], how="left")
    df["open_adj"] = df["open_raw"] * df["open_adj_factor"] / df["base_adj"] * df["fx_open"]
    df["close_adj"] = df["close_raw"] * df["close_adj_factor"] / df["base_adj"] * df["fx_close"]
    df["open_raw_cny"] = df["open_raw"] * df["fx_open"]
    df["close_raw_cny"] = df["close_raw"] * df["fx_close"]
    ratio = np.where(df["a_close_raw"] > 0, df["close_raw_cny"] / df["a_close_raw"], np.nan)
    df["pe_proxy"] = df["a_pe"] * ratio
    df["market_cap_proxy"] = df["a_total_mv"]
    df["name"] = df["pair_name"].where(df["pair_name"].notna() & (df["pair_name"] != ""), df["name"])
    df["market"] = "H"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_pickle(cache_path)
    return df


def compute_features(panel: pd.DataFrame) -> pd.DataFrame:
    panel = panel.sort_values(["market", "ts_code", "ym"]).copy()
    grouped = panel.groupby(["market", "ts_code"], sort=False)

    panel["price_low_36m"] = grouped["close_adj"].transform(
        lambda s: s.rolling(36, min_periods=36).min()
    )
    panel["price_high_36m"] = grouped["close_adj"].transform(
        lambda s: s.rolling(36, min_periods=36).max()
    )
    panel["flat_max_3m"] = grouped["close_adj"].transform(
        lambda s: s.rolling(3, min_periods=3).max()
    )
    panel["flat_min_3m"] = grouped["close_adj"].transform(
        lambda s: s.rolling(3, min_periods=3).min()
    )
    panel["amt_3m_avg"] = grouped["month_amount"].transform(
        lambda s: s.rolling(3, min_periods=3).mean()
    )
    panel["vol_3m_avg"] = grouped["month_vol"].transform(
        lambda s: s.rolling(3, min_periods=3).mean()
    )

    spread = panel["price_high_36m"] - panel["price_low_36m"]
    panel["price_pos_36m"] = np.where(
        spread > 0,
        (panel["close_adj"] - panel["price_low_36m"]) / spread,
        np.nan,
    )
    panel["flat_3m"] = np.where(
        panel["flat_min_3m"] > 0,
        panel["flat_max_3m"] / panel["flat_min_3m"] - 1.0,
        np.nan,
    )

    by_month_market = panel.groupby(["ym", "market"])
    panel["flat_pct_3m"] = by_month_market["flat_3m"].rank(pct=True, method="average")
    panel["amt_pct_3m"] = by_month_market["amt_3m_avg"].rank(pct=True, method="average")
    panel["vol_pct_3m"] = by_month_market["vol_3m_avg"].rank(pct=True, method="average")
    panel["ignored_score"] = (
        0.40 * (1.0 - panel["price_pos_36m"])
        + 0.20 * (1.0 - panel["flat_pct_3m"])
        + 0.20 * (1.0 - panel["amt_pct_3m"])
        + 0.20 * (1.0 - panel["vol_pct_3m"])
    )
    return panel


def build_trade_panel(a_monthly: pd.DataFrame, h_monthly: pd.DataFrame, cache_path: Path, refresh: bool) -> pd.DataFrame:
    if cache_path.exists() and not refresh:
        return pd.read_pickle(cache_path)

    a_panel = a_monthly[
        [
            "ts_code",
            "ym",
            "month_start",
            "month_end",
            "open_adj",
            "close_adj",
            "month_amount",
            "month_vol",
            "pe",
            "total_mv",
            "name",
            "market",
        ]
    ].rename(columns={"pe": "pe_proxy", "total_mv": "market_cap_proxy"})
    h_panel = h_monthly[
        [
            "ts_code",
            "ym",
            "month_start",
            "month_end",
            "open_adj",
            "close_adj",
            "month_amount",
            "month_vol",
            "pe_proxy",
            "market_cap_proxy",
            "name",
            "market",
        ]
    ]

    panel = pd.concat([a_panel, h_panel], ignore_index=True, sort=False)
    panel = compute_features(panel)
    panel["trade_date"] = panel["month_end"].dt.strftime("%Y%m%d")
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    panel.to_pickle(cache_path)
    return panel


def add_signal_flags(df: pd.DataFrame, params: StrategyParams) -> tuple[pd.Series, pd.Series]:
    pe_ok = (df["pe_proxy"] > 0) & (df["pe_proxy"] <= params.max_pe)
    mcap_ok = df["market_cap_proxy"] >= params.min_market_cap_yiyuan
    ignored_ok = (
        pe_ok
        & mcap_ok
        & (df["price_pos_36m"] <= params.buy_price_pos_max)
        & (df["flat_3m"] <= params.buy_flat_max)
        & (df["amt_pct_3m"] <= params.buy_liq_pct_max)
        & (df["vol_pct_3m"] <= params.buy_liq_pct_max)
    )
    crowded_ok = (
        pe_ok
        & mcap_ok
        & (df["price_pos_36m"] >= params.sell_price_pos_min)
        & (
            (df["amt_pct_3m"] >= params.sell_liq_pct_min)
            | (df["vol_pct_3m"] >= params.sell_liq_pct_min)
        )
    )
    return ignored_ok, crowded_ok


def as_curve(points: Iterable[tuple[str, float]]) -> list[CurvePoint]:
    return [CurvePoint(trade_date=d, equity=float(v)) for d, v in points]


def compute_monthly_risk(curve_df: pd.DataFrame) -> dict[str, float]:
    curve = as_curve(zip(curve_df["trade_date"], curve_df["equity"]))
    risk = compute_risk_metrics(curve, initial_equity=1.0, ann_factor=12)
    max_dd, dd_peak, dd_trough = compute_max_drawdown(curve)
    risk["MaxDrawdown"] = max_dd
    risk["DrawdownPeak"] = dd_peak or ""
    risk["DrawdownTrough"] = dd_trough or ""
    return risk


def prepare_month_maps(panel: pd.DataFrame) -> tuple[list[str], dict[str, pd.DataFrame]]:
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
            ["ignored_score", "market_cap_proxy"], ascending=[False, False]
        )
    return months, month_maps


def simulate_strategy(
    months: list[str],
    month_maps: dict[str, pd.DataFrame],
    params: StrategyParams,
) -> tuple[pd.DataFrame, dict[str, Any], pd.DataFrame]:
    if len(months) < 2:
        raise RuntimeError("not enough monthly data to simulate")

    holdings: list[str] = []
    trade_log: list[dict[str, Any]] = []
    curve_rows: list[dict[str, Any]] = []

    for idx in range(1, len(months)):
        signal_month = months[idx - 1]
        hold_month = months[idx]

        sig_df = month_maps[signal_month]
        hold_df = month_maps[hold_month]
        hold_df = hold_df.loc[
            hold_df["open_adj"].notna()
            & hold_df["close_adj"].notna()
            & (hold_df["open_adj"] > 0)
            & (hold_df["close_adj"] > 0)
        ]
        if sig_df.empty or hold_df.empty:
            continue

        buy_ok, sell_ok = add_signal_flags(sig_df, params)
        hold_index = hold_df.index

        retained: list[str] = []
        sold: list[str] = []
        for sym in holdings:
            if sym not in sig_df.index or sym not in hold_index:
                sold.append(sym)
                continue
            if bool(sell_ok.loc[sym]):
                sold.append(sym)
                continue
            retained.append(sym)

        slots = params.max_positions - len(retained)
        available_mask = buy_ok & (~sig_df.index.isin(retained)) & sig_df.index.isin(hold_index)
        available = sig_df.loc[available_mask]
        buys = available.head(max(0, slots)).index.tolist()
        target = retained + buys

        if not target:
            curve_rows.append(
                {
                    "ym": hold_month,
                    "trade_date": str(hold_df["trade_date"].max()),
                    "equity": curve_rows[-1]["equity"] if curve_rows else 1.0,
                    "position_count": 0,
                    "month_return": 0.0,
                }
            )
            holdings = []
            continue

        month_returns = hold_df.loc[target, "close_adj"] / hold_df.loc[target, "open_adj"] - 1.0
        month_return = float(month_returns.mean())
        prev_equity = curve_rows[-1]["equity"] if curve_rows else 1.0
        equity = prev_equity * (1.0 + month_return)

        trade_log.append(
            {
                "signal_month": signal_month,
                "hold_month": hold_month,
                "sold": ",".join(sold),
                "retained": ",".join(retained),
                "bought": ",".join(buys),
                "target": ",".join(target),
                "position_count": len(target),
                "month_return": month_return,
            }
        )
        curve_rows.append(
            {
                "ym": hold_month,
                "trade_date": str(hold_df["trade_date"].max()),
                "equity": equity,
                "position_count": len(target),
                "month_return": month_return,
            }
        )
        holdings = target

    curve_df = pd.DataFrame(curve_rows)
    if curve_df.empty:
        raise RuntimeError("simulation returned empty curve")
    active_curve = curve_df.loc[curve_df["position_count"] > 0].copy()
    metric_curve = active_curve if not active_curve.empty else curve_df
    risk = compute_monthly_risk(metric_curve)
    summary: dict[str, Any] = {
        **asdict(params),
        **risk,
        "FinalEquity": float(curve_df["equity"].iloc[-1]),
        "StartDate": str(metric_curve["trade_date"].iloc[0]),
        "EndDate": str(metric_curve["trade_date"].iloc[-1]),
        "Months": int(len(metric_curve)),
        "CalendarMonths": int(len(curve_df)),
        "AvgPositions": float(metric_curve["position_count"].mean()),
        "Trades": int(sum(1 for row in trade_log if row["bought"])),
    }
    return curve_df, summary, pd.DataFrame(trade_log)


def load_benchmark_monthly(con: sqlite3.Connection, start_date: str, end_date: str) -> pd.DataFrame:
    hs300 = pd.read_sql_query(
        f"""
        SELECT trade_date, ts_code, close
        FROM index_daily
        WHERE ts_code='000300.SH' AND trade_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY trade_date
        """,
        con,
    )
    ixic = pd.read_sql_query(
        f"""
        SELECT trade_date, ts_code, close
        FROM index_global
        WHERE ts_code='IXIC' AND trade_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY trade_date
        """,
        con,
    )
    raw = pd.concat([hs300, ixic], ignore_index=True)
    raw["trade_date"] = pd.to_datetime(raw["trade_date"], format="%Y%m%d")
    raw["ym"] = raw["trade_date"].dt.strftime("%Y%m")
    month_end = (
        raw.sort_values("trade_date")
        .groupby(["ts_code", "ym"], as_index=False)
        .tail(1)
        .sort_values(["ts_code", "trade_date"])
    )
    month_end["nav"] = month_end.groupby("ts_code")["close"].transform(lambda s: s / s.iloc[0])
    month_end["trade_date"] = month_end["trade_date"].dt.strftime("%Y%m%d")
    return month_end[["ts_code", "trade_date", "nav"]]


def run_grid_search(months: list[str], month_maps: dict[str, pd.DataFrame]) -> pd.DataFrame:
    grid = product(
        [0.20, 0.25, 0.30],
        [0.10, 0.15, 0.20],
        [0.20, 0.30, 0.40],
        [0.75, 0.80, 0.85],
        [0.65, 0.75, 0.85],
    )
    rows: list[dict[str, Any]] = []
    for buy_price_pos_max, buy_flat_max, buy_liq_pct_max, sell_price_pos_min, sell_liq_pct_min in grid:
        params = StrategyParams(
            buy_price_pos_max=buy_price_pos_max,
            buy_flat_max=buy_flat_max,
            buy_liq_pct_max=buy_liq_pct_max,
            sell_price_pos_min=sell_price_pos_min,
            sell_liq_pct_min=sell_liq_pct_min,
        )
        curve_df, summary, _ = simulate_strategy(months, month_maps, params)
        rows.append(
            {
                **summary,
                "CompositeScore": float(
                    summary["Sharpe"] * 0.50
                    + summary["CAGR"] * 2.00
                    + summary["MaxDrawdown"] * 0.75
                    + math.log(summary["FinalEquity"])
                ),
                "MinEquity": float(curve_df["equity"].min()),
            }
        )
    res = pd.DataFrame(rows)
    res = res.sort_values(
        ["CompositeScore", "Sharpe", "CAGR", "MaxDrawdown"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)
    return res


def plot_curves(
    curve_df: pd.DataFrame,
    benchmarks: pd.DataFrame,
    out_path: Path,
    title: str,
) -> None:
    fig, ax = plt.subplots(figsize=(12, 7))
    ax.plot(curve_df["trade_date"], curve_df["equity"], label="Strategy", linewidth=2.6, color="#b22222")
    for ts_code, label, color in [
        ("000300.SH", "CSI 300", "#1f77b4"),
        ("IXIC", "NASDAQ", "#2ca02c"),
    ]:
        bench = benchmarks.loc[benchmarks["ts_code"] == ts_code]
        if bench.empty:
            continue
        ax.plot(bench["trade_date"], bench["nav"], label=label, linewidth=1.8, color=color)
    ax.set_title(title)
    ax.set_ylabel("Normalized NAV")
    ax.set_xlabel("Month")
    ax.grid(alpha=0.25)
    ax.legend()
    tick_idx = np.linspace(0, len(curve_df) - 1, min(10, len(curve_df)), dtype=int)
    ax.set_xticks(curve_df["trade_date"].iloc[tick_idx])
    ax.tick_params(axis="x", rotation=30)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Research ignored/crowded A/H monthly strategy.")
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--pairs", type=Path, default=PAIRS_CSV)
    parser.add_argument("--out-dir", type=Path, default=OUTPUT_DIR)
    parser.add_argument("--refresh-cache", action="store_true")
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(args.db)

    pairs = load_pairs(args.pairs)
    fx_rates = load_fx_rates(con)
    a_monthly = build_monthly_a(con, args.out_dir / "cache" / "a_monthly.pkl", args.refresh_cache)
    h_monthly = build_monthly_h(
        con,
        args.out_dir / "cache" / "h_monthly.pkl",
        args.refresh_cache,
        fx_rates=fx_rates,
        pairs=pairs,
        a_monthly=a_monthly,
    )
    panel = build_trade_panel(
        a_monthly,
        h_monthly,
        args.out_dir / "cache" / "trade_panel.pkl",
        args.refresh_cache,
    )

    months, month_maps = prepare_month_maps(panel)

    search_res = run_grid_search(months, month_maps)
    search_res.to_csv(args.out_dir / "grid_search_results.csv", index=False)

    best = search_res.iloc[0].to_dict()
    best_params = StrategyParams(
        buy_price_pos_max=float(best["buy_price_pos_max"]),
        buy_flat_max=float(best["buy_flat_max"]),
        buy_liq_pct_max=float(best["buy_liq_pct_max"]),
        sell_price_pos_min=float(best["sell_price_pos_min"]),
        sell_liq_pct_min=float(best["sell_liq_pct_min"]),
    )
    curve_df, summary, trade_log = simulate_strategy(months, month_maps, best_params)
    curve_df.to_csv(args.out_dir / "best_equity_curve.csv", index=False)
    trade_log.to_csv(args.out_dir / "best_trade_log.csv", index=False)

    benchmarks = load_benchmark_monthly(
        con,
        start_date=str(curve_df["trade_date"].iloc[0]),
        end_date=str(curve_df["trade_date"].iloc[-1]),
    )
    benchmarks.to_csv(args.out_dir / "benchmarks_monthly.csv", index=False)

    plot_curves(
        curve_df,
        benchmarks,
        args.out_dir / "best_strategy_vs_benchmarks.png",
        title="Ignored/Crowded A-H Monthly Strategy",
    )

    top10 = search_res.head(10).to_dict(orient="records")
    write_json(
        args.out_dir / "best_summary.json",
        {
            "data_constraint": {
                "a_pe_history_start": "20170614",
                "note": "Strict PE filter uses bak_daily_a; H-share PE is approximated from paired A-share PE.",
            },
            "best_params": asdict(best_params),
            "best_summary": summary,
            "top10": top10,
        },
    )

    print("Best params:")
    print(json.dumps(asdict(best_params), ensure_ascii=False, indent=2))
    print("Best summary:")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"Outputs written to: {args.out_dir}")


if __name__ == "__main__":
    main()
