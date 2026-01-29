# 多市场日频数据统一同步说明 (fx_daily / daily_a / adj_factor_a / bak_daily_a / daily_h / adj_factor_h / index_daily / index_global)

对应统一脚本: `src/data_fetcher/tushare_sync_daily.py`
目标 SQLite 数据库: `data/data.sqlite`
辅助控制表: `sync_date` (增量进度)

涵盖数据来源 (TuShare 接口 / 官方文档 doc_id):
- 外汇日线: `fx_daily` (doc_id=179)  代码来源 `fx_basic` 全量，可 CLI 过滤
- A 股日线: `daily`  → 本地表名 `daily_a` (doc_id=27)
- A 股复权因子: `adj_factor` → 本地表名 `adj_factor_a`
- A 股特色拓展: `bak_daily` → 本地表名 `bak_daily_a` (doc_id=255)
- 港股日线: `hk_daily` → 本地表名 `daily_h` (doc_id=192)
- 港股复权因子: `hk_daily_adj` → 本地表名 `adj_factor_h`
- 国内指数日线: `index_daily` → 本地表名 `index_daily` (当前仅: 000300.SH 沪深300)
- 国际指数日线: `index_global` → 本地表名 `index_global` (当前仅: HSI 恒生指数, IXIC 纳斯达克综合)

表名与接口名映射统一由脚本中的 `TABLE_CONFIG` 定义; 每个表具有:
- `api_name`  (TuShare 接口)
- `fields`    (请求字段列表, 必含 ts_code, trade_date)
- `stock_table` 或 `ts_codes` (代码池来源: 基础表 / 显式代码列表)
- FX 使用 `stock_table=fx_basic`；指数暂用显式 `ts_codes`，后续可改为基础表。
- 可选 `limit` / `sleep` / `sleep_on_fail` (分页与节流参数, 覆盖全局默认)

---
## 1. 增量同步核心机制

(内容与旧版一致，补充指数与 FX 动态代码池要点)

1. 代码池来源:
   - FX: 读取 `fx_basic` 全量，再按 CLI 过滤。
   - 指数: 当前通过显式列表 (`index_daily`: 000300.SH; `index_global`: HSI, IXIC)。
   - A/H: 读取各自基础表。
2~9. (同旧版逻辑)。

---
## 2. 各数据表字段说明

### 2.1 外汇日线: `fx_daily` (TuShare: fx_daily, doc_id=179)
| 字段 | 说明 | 备注 |
|------|------|------|
| ts_code    | 货币对代码 (如 USDCNH.FXCM) | 代码来自 `fx_basic` |
| trade_date | 日期 YYYYMMDD | FX 周末多为空 |
| bid_open   | 买价开盘 | |
| bid_close  | 买价收盘 | |
| bid_high   | 买价最高 | |
| bid_low    | 买价最低 | |
| ask_open   | 卖价开盘 | |
| ask_close  | 卖价收盘 | |
| ask_high   | 卖价最高 | |
| ask_low    | 卖价最低 | |
| tick_qty   | Ticks 数量 / 成交笔数 | |

### 2.2 A 股日线: `daily_a`
(同前)

### 2.3 A 股复权因子: `adj_factor_a`
(同前)

### 2.4 A 股特色拓展: `bak_daily_a`
(同前)

### 2.5 港股日线: `daily_h`
(同前)

### 2.6 港股复权因子: `adj_factor_h`
(同前)

### 2.7 国内指数日线: `index_daily` (TuShare: index_daily)
当前仅同步: 000300.SH (沪深300)。
| 字段 | 说明 |
|------|------|
| ts_code | 指数代码 (000300.SH) |
| trade_date | 日期 |
| open/high/low/close | 当日 OHLC |
| pre_close | 昨收 |
| change | 涨跌额 |
| pct_chg | 涨跌幅(%) |
| vol | 成交量 (接口单位) |
| amount | 成交额 (接口单位) |

### 2.8 国际指数日线: `index_global` (TuShare: index_global)
当前仅同步: HSI (恒生指数), IXIC (纳斯达克综合)。
| 字段 | 说明 |
|------|------|
| ts_code | 指数代码 (HSI / IXIC) |
| trade_date | 日期 |
| open/close/high/low | 当日价格 (注意字段顺序: open, close, high, low) |
| pre_close | 昨收 |
| change | 涨跌额 |
| pct_chg | 涨跌幅(%) |
| swing | 振幅(%) |
| vol | 成交量 (接口单位) |

### 2.9 同步控制表: `sync_date`
(同前)

---
## 3. 去重 / 幂等与数据质量
(同前; 指数表同样建立唯一索引避免重复)

---
## 4. 常见查询示例

最近 30 日沪深300 收盘:
```sql
SELECT trade_date, close FROM index_daily WHERE ts_code='000300.SH' ORDER BY trade_date DESC LIMIT 30;
```
恒生指数与纳指最近 10 日涨跌幅对比:
```sql
SELECT a.trade_date,
       h.pct_chg AS hsi_pct,
       n.pct_chg AS ixic_pct
FROM (SELECT DISTINCT trade_date FROM index_global WHERE trade_date>=strftime('%Y%m%d', date('now','-15 day'))) a
LEFT JOIN index_global h ON a.trade_date=h.trade_date AND h.ts_code='HSI'
LEFT JOIN index_global n ON a.trade_date=n.trade_date AND n.ts_code='IXIC'
ORDER BY a.trade_date DESC LIMIT 10;
```
指数与 A 股基准对齐 (示例: 沪深300 + 恒生):
```sql
SELECT d.trade_date, c.close AS hs300_close, g.close AS hsi_close
FROM (SELECT DISTINCT trade_date FROM index_daily) d
LEFT JOIN index_daily c ON d.trade_date=c.trade_date AND c.ts_code='000300.SH'
LEFT JOIN index_global g ON d.trade_date=g.trade_date AND g.ts_code='HSI'
ORDER BY d.trade_date DESC LIMIT 60;
```

---
## 5. LLM 使用提示补充 (指数)
- 指数覆盖有限，当前仅 3 个: 000300.SH (沪深300), HSI (恒生), IXIC (纳斯达克综合)。
- 若用户请求其它指数，可提示“尚未配置 ts_code，可在 TABLE_CONFIG 增加或改为显式 ts_codes”。
- 国内/国际指数字段顺序略有区别：`index_global` 含 `swing`，且字段顺序 open, close, high, low。

---
## 6. 运维与扩展建议 (新增指数)
| 场景 | 操作 |
|------|------|
| 增加更多国内指数 | 在 `index_daily.ts_codes` 追加代码 (如 000905.SH 中证500) |
| 增加更多国际指数 | 在 `index_global.ts_codes` 追加 (如 SPX, DJI 等; 需确认 TuShare ts_code) |
| 改为动态基础表 | 为指数建立 `index_basic` 脚本后把 `ts_codes` 改为 `stock_table` |

---
其余章节与原说明一致。
