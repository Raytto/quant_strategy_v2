# A/H 溢价计算脚本说明 (`ah_premium.py`)

脚本位置: `src/data_processor/ah_premium.py`
输出库: `data/data_processed.sqlite`
输出表: `ah_premium`

## 1. 目标
基于已同步的 A 股与 H 股日线收盘价, 通过 USD/CNH 与 USD/HKD 两条美元交叉汇率将 H 股价格折算为人民币, 计算 A/H 溢价时序, 供监控、策略与特征工程使用。

溢价定义:
```
HKD→CNY 汇率 (hk_to_cny) = (USD/CNH 中间价) / (USD/HKD 中间价)
H 股折算人民币价 close_h_cny = close_h_hkd * hk_to_cny
premium_ratio = close_a / close_h_cny
premium_pct   = (premium_ratio - 1) * 100
```

## 2. 上游依赖
| 数据 | 来源 | 说明 |
|------|------|------|
| daily_a | `data/data.sqlite` | A 股日线 (收盘价 RMB) |
| daily_h | `data/data.sqlite` | 港股日线 (收盘价 HKD) |
| fx_daily | `data/data.sqlite` | 汇率 (需含 `USDCNH.FXCM` 与 `USDHKD.FXCM`) |
| ah_codes.csv | `data/ah_codes.csv` | A/H 对应映射 (name, cn_code, hk_code) |

缺失任一组成部分的交易日/股票将被跳过 (不插入行)。

## 3. 输出表结构
| 列 | 含义 |
|----|------|
| trade_date | 交易日 YYYYMMDD |
| name | 公司简称 (仅阅读辅助) |
| cn_code | A 股代码 (ts_code) |
| hk_code | H 股代码 (ts_code) |
| close_a | A 股收盘价 (未复权, RMB) |
| close_h_hkd | H 股收盘价 (HKD) |
| usd_cnh_mid | USD/CNH 中间价 = (bid_close+ask_close)/2 |
| usd_hkd_mid | USD/HKD 中间价 = (bid_close+ask_close)/2 |
| hk_to_cny | 1 HKD 兑多少 CNY = usd_cnh_mid / usd_hkd_mid |
| close_h_cny | H 股折算人民币价 |
| premium_ratio | A/H 价比 |
| premium_pct | A/H 溢价百分比 |

索引: 唯一索引 `(cn_code, trade_date)` 保证幂等。

## 4. 使用方式
全量重建 (默认):
```
python -m data_processor.ah_premium
```
增量追加 (仅新日期):
```
python -m data_processor.ah_premium --append
```
指定自定义路径:
```
python -m data_processor.ah_premium --source-db data/data.sqlite --output-db data/data_processed.sqlite --ah-csv data/ah_codes.csv
```

## 5. 追加与重建策略
- 默认直接全量重建: 简化回溯修订处理。
- `--append`: 读取现有最大 trade_date, 仅追加更晚日期。若上游发生历史修订需再次全量重建。

## 6. 常见分析示例
最近 20 日溢价滚动均值（示例）:
```sql
WITH base AS (
  SELECT cn_code, trade_date, premium_pct,
         AVG(premium_pct) OVER w AS ma
  FROM ah_premium
  WINDOW w AS (PARTITION BY cn_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
)
SELECT *, (premium_pct - ma) AS prem_minus_ma
FROM base
WHERE trade_date >= '20250101';
```
极端溢价检测:
```sql
SELECT * FROM ah_premium
WHERE ABS(premium_pct) > 40
ORDER BY ABS(premium_pct) DESC
LIMIT 100;
```

## 7. LLM 回答提示要点
1. 明确 A/H 溢价已使用交叉汇率折算 (可能与直接 HKD/CNY 汇率存在细微差异)。
2. 价格未复权: 长期比较或除权日附近需使用复权价 (可扩展后续表)。
3. 缺失行表示当日某一组成数据尚未可得, 不一定代表停牌或退市。
4. 若用户希望实时刷新, 需先运行日频同步脚本再运行本脚本。
5. premium_pct = (close_a / (close_h_hkd * hk_to_cny) - 1) * 100。

## 8. 扩展路线
| 方向 | 描述 |
|------|------|
| 直接汇率校验 | 引入 HKD/CNY 直接汇率后对比差值, 建立质量监控 |
| 复权支持 | 增加复权价溢价字段 premium_pct_fq |
| 滚动特征物化 | 预计算 rolling mean / std / zscore 减少下游重复计算 |
| 事件标注 | 标记异常高溢价或快速回归的事件窗口 |

## 9. 安全回答模板
- “溢价基于未复权收盘价与美元交叉汇率计算, 若需长期收益请使用复权价。”
- “缺失日期通常因汇率或某市场日线尚未发布, 可稍后重算补齐。”
- “上游数据回溯会影响历史溢价, 建议定期全量重建。”
