# A / H 股对应代码映射说明 (`data/ah_codes.csv`)

该文件提供已在 A 股与港股 (H 股) 双重上市公司的代码对应关系，便于：
- 跨市场价差 / 溢价 (A/H Premium) 计算
- 统一因子研究中建立同一实体的多市场特征拼接
- 大模型 (LLM) 在回答“某港股对应的 A 股代码”或反向问题时快速检索

---
## 1. 数据来源与维护
- 当前为静态手工/一次性整理(初始快照)。
- 列表中公司可能因新增、退市、拆分、重组产生变化；本文件不自动更新。
- 若需刷新：重新生成最新 A/H 对应表 (可通过名称相似 + 行业过滤 + 人工校验)，覆盖该 CSV。

建议：可在后续加入脚本，利用 `stock_basic_a` 与 `stock_basic_h` 的名称前缀 / ISIN / 全称模糊匹配半自动更新，再人工确认差异。

---
## 2. 字段说明
| 列名 | 说明 | 示例 | 备注 |
|------|------|------|------|
| name | 公司中文简称 (去除股份/集团等可能未完全标准化) | 中国石油化工股份 | 仅用于辅助阅读，不建议作为联接键 |
| cn_code | A 股统一代码 (含交易所后缀 .SH/.SZ) | 600028.SH | 可与 `daily_a` / `stock_basic_a` 关联 |
| hk_code | H 股统一代码 (含 .HK) | 00386.HK | 可与 `daily_h` / `stock_basic_h` 关联 |

主键：逻辑上以 (cn_code, hk_code) 组合唯一；`name` 非唯一且可能与官方简称不同。

---
## 3. 典型使用场景
1. A/H 溢价计算 (以港股折算为人民币再与 A 股价比较)。
2. 跨市场联动因子：比较成交量相对强度、波动率传导。
3. 事件研究：H 股/ A 股公告后另一市场价格反应。
4. 多市场一致性校验：行业轮动、资金流向差异。

---
## 4. 示例查询 (SQLite)

> 前提：先将 `data/ah_codes.csv` 导入到 SQLite 的 `ah_codes` 表（示例见文末）。

### 4.1 计算指定起始日以来 A/H 收盘价比值 (未汇率折算)
```sql
WITH ah AS (
  SELECT a.trade_date, m.cn_code, m.hk_code,
         a.close AS close_a,
         h.close AS close_h
  FROM ah_codes m
  JOIN daily_a a ON a.ts_code = m.cn_code
  JOIN daily_h h ON h.ts_code = m.hk_code AND h.trade_date = a.trade_date
  WHERE a.trade_date >= '20240101'
)
SELECT *, close_a / NULLIF(close_h,0) AS a_h_ratio
FROM ah
ORDER BY trade_date, cn_code;
```

### 4.2 A/H 溢价加入汇率折算 (港股 HKD → CNY, 简化用 USDCNH 近似)
```sql
WITH rate AS (
  SELECT trade_date,
         (bid_close + ask_close)/2 AS usd_cnh_mid
  FROM fx_daily WHERE ts_code='USDCNH.FXCM'
), hk_cny AS (
  SELECT h.trade_date, h.ts_code, h.close * r.usd_cnh_mid AS close_h_cny
  FROM daily_h h LEFT JOIN rate r ON h.trade_date = r.trade_date
), joined AS (
  SELECT m.cn_code, m.hk_code, a.trade_date,
         a.close AS close_a,
         k.close_h_cny
  FROM ah_codes m
  JOIN daily_a a ON a.ts_code = m.cn_code
  JOIN hk_cny k ON k.ts_code = m.hk_code AND k.trade_date = a.trade_date
)
SELECT *, close_a / NULLIF(close_h_cny,0) AS premium_a_over_h
FROM joined
ORDER BY trade_date, cn_code;
```

### 4.3 缺少对应行情的对照检查
```sql
SELECT m.*
FROM ah_codes m
LEFT JOIN daily_a a ON a.ts_code = m.cn_code
LEFT JOIN daily_h h ON h.ts_code = m.hk_code
WHERE a.ts_code IS NULL OR h.ts_code IS NULL;
```

---
## 5. LLM 使用提示
- 若用户给出 6 位数字且问“对应 H 股” → 在该 CSV 中查 `cn_code`，返回 `hk_code`；反向同理。
- 回答中应提示数据可能滞后：列表不随新增/退市自动更新。
- 计算溢价应说明是否做了汇率折算与复权处理：
  - 价格是否用前复权价 (需结合 `adj_factor_*`)
  - 汇率是否使用当日 FX 中间价或其他来源
- 若某对未在清单中：提示“该公司可能非 A/H 双重上市或列表未更新”。
- 不要用 `name` 做准确匹配 (存在简写 / 变更 / 同名风险)。

---
## 6. 质量与维护建议
| 场景 | 操作 | 备注 |
|------|------|------|
| 更新映射 | 重新生成 CSV 覆盖 | 确认差异后备份旧版本 |
| 增量校验 | 比较新旧 CSV 集合差异 | `EXCEPT` / `MINUS` 查询 |
| 自动化生成 | 利用名称前缀 + 行业 + 市值过滤 | 仍需人工复核 |
| 退市清理 | 移除对应代码行 | 保留历史需加状态列 |
| 增加字段 | 增加 `list_date_a`, `list_date_h`, `industry` | 便于筛选/统计 |

可扩展：增加“是否沪/深港通标的”、“是否纳入主要指数”标签，丰富横截面研究。

---
## 7. 安全回答模板示例
- “该 A/H 对照表是静态快照，可能未包含最新双重上市变更。”
- “进行 A/H 溢价分析时请先确认是否已进行汇率与复权调整。”
- “如需实时对照，请基于最新基础表按名称/ISIN 重新匹配生成。”

---
若要将本清单结构化落库，可导入 SQLite（sqlite3 CLI 示例）：
```sql
.mode csv
.import data/ah_codes.csv ah_codes
CREATE UNIQUE INDEX IF NOT EXISTS ah_codes_uq ON ah_codes(cn_code, hk_code);
```
