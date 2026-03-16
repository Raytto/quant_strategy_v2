# quant-strategy-v2

## Conda (myqs)

### 1) Create / update env

- `conda env create -f environment.yml` (first time)
- `conda env update -n myqs -f environment.yml` (update)

### 1.5) Activate (optional)

- `conda activate myqs`

### 2) Configure token

- `cp .env.example .env` and fill `tushare_api_token`

### 3) Fetch data

- `bash scripts/fetch_data.sh`
- Or (no conda): `python scripts/fetch_data.py`
- Or (explicit): `conda run -n myqs python scripts/fetch_data.py`

### 3.5) Backfill a few ETFs (optional)

If some ETFs only have ~1y history in `data/data.sqlite`, you can rebuild just those symbols for `etf_daily` + `adj_factor_etf`:

- `conda run -n myqs python -c "import _bootstrap; from data_fetcher.tushare_sync_daily import main; main()" -t etf_daily adj_factor_etf --ts-codes 159001.SZ 159922.SZ 159934.SZ 159941.SZ 159905.SZ --rebuild`

### 4) Run tests

- `conda run -n myqs pytest`

## Strategy Framework Rules

All new production strategies must be written on top of `qs.backtester` and must use framework-managed data access. The goal is simple: strategy code only decides signals and target holdings; the framework owns history visibility, execution, valuation, cash, fees, and write-offs.

### Required pattern

- Put formal strategies in `src/qs/strategy/`.
- Implement `on_bar_ctx(self, ctx: StrategyContext)`.
- Read historical data only from `ctx.history`.
- Read static metadata only from `ctx.reference`.
- Read current execution / mark prices only through `ctx.current_price_map(...)` or explicit `PriceRequest`.
- Submit holdings with `ctx.rebalance_to_weights(...)`.
- Submit valuation prices with `ctx.set_mark_request(...)`.
- Handle delist / forced zeroing with `ctx.request_write_off(...)`.

### Hard rules

- Do not open SQLite connections inside strategy modules.
- Do not import or use `connect_sqlite` inside new strategies.
- Do not access `Broker`, `DataFeed`, or raw bars directly for strategy logic if `StrategyContext` can provide the data.
- Do not implement hand-written `run_backtest()`, `simulate_strategy()`, equity rolling, or manual PnL bookkeeping inside strategy files.
- Do not read trade-date data as signal-date data. Signal logic must only see `ctx.signal_date` and earlier history.
- Do not silently skip missing execution prices. If a rebalance cannot be executed cleanly, let the framework reject it.

### Practical template

```python
from qs.backtester.market import PriceRequest, StrategyContext


class ExampleStrategy:
    def __init__(self):
        self.open_request = PriceRequest(table="daily_a", field="open", exact=True)
        self.close_request = PriceRequest(table="daily_a", field="close", exact=False)

    def on_bar_ctx(self, ctx: StrategyContext) -> None:
        ctx.set_mark_request(self.close_request)
        if ctx.signal_date is None:
            return

        row = ctx.history.get_dataset_values(
            table="daily_a",
            symbols=["601628.SH"],
            fields=["pct_chg"],
            trade_date=ctx.signal_date,
            exact=True,
        ).get("601628.SH")
        if not row or row["pct_chg"] is None:
            return

        if float(row["pct_chg"]) >= 1.0:
            ctx.rebalance_to_weights(
                {"601628.SH": 1.0},
                execution_request=self.open_request,
            )
```

### Cross-sectional strategies

If a strategy needs a full snapshot or universe scan, use framework market-data APIs such as:

- `ctx.history.get_snapshot_rows(...)`
- `ctx.history.get_latest_trade_date(...)`
- `ctx.reference.get_values(..., symbols=None, ...)`

Do not fall back to ad-hoc SQL in the strategy.

### Legacy code policy

- `src/qs/strategy/ignored_stock_strategy.py` is legacy research code and is not a valid framework strategy.
- Legacy experiments may stay for reference, but they must be clearly marked legacy and must not be used as templates for new work.
- New Codex changes should prefer existing framework-based strategies such as `etf_equal_weight_annual.py`, `etf_min_premium_weekly.py`, `ah_premium_quarterly.py`, `low_pe_quarterly.py`, and `ignored_crowded_ah_monthly.py`.
