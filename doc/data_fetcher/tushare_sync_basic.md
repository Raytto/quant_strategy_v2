# TuShare 基础数据同步（A/H/ETF/FX）`tushare_sync_basic.py`

说明：本文件为 `doc/data/tushare_sync_basic.md` 的同内容副本，方便在 `doc/data_fetcher/` 目录下查阅。

对应脚本：`src/data_fetcher/tushare_sync_basic.py`  
目标数据库：`data/data.sqlite`

该脚本用于同步（增量插入、忽略已存在）四张“代码池/资产维度”基础表，供后续日频增量同步脚本 `tushare_sync_daily.py` 使用：
- A 股基础：`stock_basic_a`（TuShare `stock_basic`）
- 港股基础：`stock_basic_h`（TuShare `hk_basic`）
- 外汇基础：`fx_basic`（TuShare `fx_obasic`，供 `fx_daily` 动态代码池使用）
- ETF 基础：`etf_basic`（TuShare `etf_basic`）

---

## 0. 前置条件

1) 配置 TuShare Token  
在项目根目录复制并填写：
- `cp .env.example .env`
- 设置 `tushare_api_token=...`（脚本也兼容环境变量 `TUSHARE_API_TOKEN`）

2) 确保可导入 `src/` 下的包  
推荐直接使用项目提供的入口：
- `bash scripts/fetch_data.sh`
- 或 `python scripts/fetch_data.py`

如需单独跑本脚本（仅基础表）：
- `python -c "import _bootstrap; from data_fetcher.tushare_sync_basic import data_sync; data_sync()"`

---

## 1. 同步产物（SQLite 表）

### 1.1 `stock_basic_a`
数据源：TuShare `stock_basic`  
用途：A 股日线/复权因子/特色行情等下游表的 `ts_code` 代码池与基础元数据。

字段（脚本请求字段，见 `MARKET_CONFIG['A']['fields']`）：
- `ts_code, symbol, name, area, industry, cnspell, market, list_date, act_name, act_ent_type, fullname, enname, exchange, curr_type, list_status, delist_date, is_hs`

### 1.2 `stock_basic_h`
数据源：TuShare `hk_basic`  
用途：港股日线/复权因子下游表的 `ts_code` 代码池与基础元数据。

字段（见 `MARKET_CONFIG['H']['fields']`）：
- `ts_code, name, fullname, enname, cn_spell, market, list_status, list_date, delist_date, trade_unit, isin, curr_type`

### 1.3 `fx_basic`
数据源：TuShare `fx_obasic`  
用途：外汇日线 `fx_daily` 的动态代码池（不再假设固定少数货币对）。

字段（见 `MARKET_CONFIG['FX']['fields']`）：
- `ts_code, name, classify, exchange, min_unit, max_unit, pip, pip_cost, traget_spread, min_stop_distance, trading_hours, break_time`

说明：
- `traget_spread` 为接口原始字段名（可能存在拼写差异），脚本按文档字段名原样入库。

### 1.4 `etf_basic`
数据源：TuShare `etf_basic`  
用途：ETF 代码池与基础元数据（基金经理、托管人、费率、跟踪指数等），可供后续 ETF 行情/持仓等同步脚本使用。

字段（见 `MARKET_CONFIG['ETF']['fields']`）：
- `ts_code, csname, extname, cname, index_code, index_name, setup_date, list_date, list_status, exchange, mgr_name, custod_name, mgt_fee, etf_type`

---

## 2. 核心逻辑（与脚本一致）

1) 四个市场配置集中在 `MARKET_CONFIG`：包含 `api_name / params / fields / table`。  
2) `_fetch_table()` 分页拉取：
   - 每页 `limit=3000`（`LIMIT`），使用 `offset` 翻页；
   - 每页失败最多重试 `MAX_RETRY=3`，重试等待 `SLEEP*2`；
   - 正常翻页间隔 `SLEEP=0.6s`（约每分钟 100 次调用）。
3) 拉取完成后会对 `ts_code` 做去重（防止分页重复/接口异常）。  
4) `_upsert()` 的落库策略是“增量插入（忽略重复）”：
   - 首次运行会创建表结构（按本次拉取字段建表）；
   - 若存在 `ts_code` 列，会创建唯一索引：
     - `stock_basic_a_uq` / `stock_basic_h_uq` / `fx_basic_uq` / `etf_basic_uq`
   - 后续运行使用 `INSERT OR IGNORE` 插入新行；已存在的 `ts_code` 不会被更新。
5) 日志：脚本在未配置根 logger 时强制启用 `INFO` 级别，确保被 `import` 调用也可见进度日志。

---

## 3. 常量与可调参数

脚本内置常量：
- `SQLITE_PATH = data/data.sqlite`
- `LIMIT = 3000`
- `MAX_RETRY = 3`
- `SLEEP = 0.6`

如需扩展资产类别/字段：
- 直接在 `src/data_fetcher/tushare_sync_basic.py` 的 `MARKET_CONFIG` 增加或调整对应配置。

---

## 4. 常见问题

### 4.1 报错：Missing `tushare_api_token`
未设置 token。按“前置条件”创建 `.env` 并填写 `tushare_api_token`。

### 4.2 报错：缺少 `stock_basic_*` / `fx_basic`
这是日频增量脚本 `tushare_sync_daily.py` 的前置基础表；请先跑本脚本或直接运行 `python scripts/fetch_data.py`。

---

## 5. SQL 自检示例

基础表规模：
```sql
SELECT 'A' AS mkt, COUNT(*) AS n FROM stock_basic_a
UNION ALL
SELECT 'H' AS mkt, COUNT(*) AS n FROM stock_basic_h
UNION ALL
SELECT 'FX' AS mkt, COUNT(*) AS n FROM fx_basic
UNION ALL
SELECT 'ETF' AS mkt, COUNT(*) AS n FROM etf_basic;
```

检查主键重复（理论上应为 0 行）：
```sql
SELECT ts_code, COUNT(*) c FROM stock_basic_a GROUP BY 1 HAVING c > 1;
```
