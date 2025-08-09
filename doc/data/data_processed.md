# 处理结果数据库 (`data/data_processed.duckdb`)

该数据库存放在原始行情基础数据 (`data/data.duckdb`) 之上派生的分析 / 因子 / 指标等结果表。当前已包含:

| 表名 | 主键 | 说明 | 上游依赖 |
|------|------|------|----------|
| ah_premium | cn_code + trade_date | A/H 溢价历史 (未复权价, HKD 通过 USD 交叉汇率折算为 CNY) | daily_a, daily_h, fx_daily, ah_codes.csv |

> 后续可在此库中加入：复权价物化 (daily_a_fq)、流动性因子截面、行业轮动信号、事件窗口回测临时结果等。

---
## 1. `ah_premium` 表结构与语义
| 列名 | 类型 | 说明 |
|------|------|------|
| trade_date | TEXT(YYYYMMDD) | 交易日 |
| name | TEXT | 公司简称 (来自映射, 仅阅读辅助) |
| cn_code | TEXT | A 股 ts_code |
| hk_code | TEXT | H 股 ts_code |
| close_a | DOUBLE | A 股收盘价 (RMB, 未复权) |
| close_h_hkd | DOUBLE | H 股收盘价 (HKD, 未复权) |
| usd_cnh_mid | DOUBLE | USD/CNH 中间价 = (bid_close+ask_close)/2 |
| usd_hkd_mid | DOUBLE | USD/HKD 中间价 = (bid_close+ask_close)/2 |
| hk_to_cny | DOUBLE | 1 HKD 兑多少 CNY = usd_cnh_mid / usd_hkd_mid |
| close_h_cny | DOUBLE | H 股折算人民币价格 = close_h_hkd * hk_to_cny |
| premium_ratio | DOUBLE | 价比 = close_a / close_h_cny |
| premium_pct | DOUBLE | 溢价百分比 = (premium_ratio - 1) * 100 |

### 1.1 计算口径
- 汇率链: HKD→CNY 通过 USD 做交叉: (USD/CNH) / (USD/HKD)。
- 价格均为原始收盘价 (未做复权)。若需长期收益或复权比较，请另行引入 `adj_factor_*` 复权。
- 缺失策略: 任一组成部分缺失 (A/H 价格、两条汇率) 则该交易日该股票不入表。

### 1.2 与上游刷新关系
- 每次运行 `ah_premium.py` (全量或追加) 读取当下 `data/data.duckdb` 最新数据。
- 若上游出现回溯修订，需要执行全量重建 (默认模式) 以重新计算历史。
- 追加模式仅插入新日期 ( > 当前表内最大 trade_date )。

### 1.3 典型用途
1. 监控 A/H 溢价时序与极值分布。
2. 构建溢价均值回复策略 (结合滚动 Z-Score)。
3. 事件前后跨市场价格反应差异分析。
4. 作为特征输入：将最近 N 日溢价、溢价变化率纳入多因子模型。

### 1.4 示例查询
最近 5 日某 A/H 对应股票溢价:
```sql
SELECT trade_date, premium_pct
FROM ah_premium
WHERE cn_code='600028.SH'
ORDER BY trade_date DESC
LIMIT 5;
```
溢价异常值检视 (绝对百分比 > 50%):
```sql
SELECT * FROM ah_premium
WHERE ABS(premium_pct) > 50
ORDER BY ABS(premium_pct) DESC
LIMIT 50;
```
溢价均值回复信号 (简单 Z-Score):
```sql
WITH base AS (
  SELECT cn_code, trade_date, premium_pct,
         AVG(premium_pct) OVER w AS ma,
         STDDEV(premium_pct) OVER w AS sd
  FROM ah_premium
  WINDOW w AS (PARTITION BY cn_code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
)
SELECT *, (premium_pct - ma)/NULLIF(sd,0) AS z
FROM base
WHERE trade_date >= '20240101';
```

---
## 2. LLM 使用提示
- 生成 SQL 时如需最新数据, 可建议用户先运行同步脚本 + 本处理脚本全量重建。
- 若用户询问“溢价”定义, 明确: premium_pct = (A股收盘价 / (H股收盘价 * hk_to_cny) - 1) * 100。
- 注意未复权：若涉及分红/配股影响跨期比较，应提示进行复权价替换或剔除除权日。
- 若某日期缺失记录，常见原因是汇率或其中一个市场尚未有数据 (晚到)。

---
## 3. 维护与扩展建议
| 场景 | 建议 |
|------|------|
| 引入复权价 | 增加列 close_a_fq, close_h_fq (需先计算港股复权) |
| 直接汇率 | 若后续有 HKD/CNY 直接行情, 比较交叉与直接差异用于质量监控 |
| 回溯修订 | 定期对比最新全量重建与上一版本差异 (EXCEPT) |
| 性能优化 | 为 (cn_code, trade_date) 已建唯一索引; 可加 trade_date 单列索引提升范围扫描 |
| 新特征 | 添加溢价滚动均值/标准差、Z 分数物化减少下游实时计算 |

---
如需新增其他派生表，请在 `doc/data/data_processed.md` 增补对应章节，以保持数据资产自描述完善。
