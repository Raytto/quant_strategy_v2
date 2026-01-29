# quant-strategy-v2

## Conda (myqs)

### 1) Create / update env

- `conda env create -f environment.yml` (first time)
- `conda env update -n myqs -f environment.yml` (update)

### 2) Configure token

- `cp .env.example .env` and fill `tushare_api_token`

### 3) Fetch data

- `bash scripts/fetch_data.sh`
