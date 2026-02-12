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
