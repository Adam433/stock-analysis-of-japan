# RPS 语义契约

状态: approved
定义版本: `rps-v1-2026-04-14`
适用范围: Screening / Chart Detail / Backtesting / Explainability

## 1. 目的

本文件冻结 MVP 阶段 RPS 相关能力的业务定义与数据契约，确保筛选、图表展示、回测与解释链使用同一套语义，而不是各自推断。

## 2. 正式定义

### 2.1 支持周期

当前 MVP 支持以下三个 lookback:

- `RPS 50`
- `RPS 120`
- `RPS 250`

### 2.2 价格口径

RPS 计算使用以下价格解析规则:

1. 优先使用 `adj_close`
2. 如果 `adj_close` 缺失，则回退到 `close`
3. 如果两者都缺失，或该行情行的 `data_status` 为 `unavailable`，则该证券在该交易日不参与对应 RPS 计算

该规则与当前实现一致，来源于 `apps/api/src/stockanalyse_api/services/factor_materialization.py` 中的 `_resolve_price` 与物化逻辑。

### 2.3 原始强度值

对每个证券、每个 lookback，在可计算时使用下式得到原始强度值:

`relative_strength = (current_price / prior_price) - 1`

其中:

- `current_price` 为当前交易日按价格口径解析后的价格
- `prior_price` 为向前追溯 `lookback` 个有效历史点后的价格

### 2.4 排名宇宙

某一交易日、某一 lookback 的 RPS 排名宇宙定义为:

- 当日存在可用价格
- 历史窗口足以回看该 lookback
- 该证券未因 `unavailable` 或缺价而被排除

换言之，RPS 不是对“全市场静态名单”强行排名，而是对“该交易日该 lookback 下可计算的证券集合”做横截面排名。

### 2.5 百分位得分

对每个交易日、每个 lookback，将可计算证券按 `relative_strength` 从低到高排序。

若可计算证券数量为:

- `0`: 不产生该交易日该 lookback 的任何 RPS 值
- `1`: 唯一证券得分为 `100`
- `n > 1`: 使用 `rank_index / (n - 1) * 100` 计算百分位

分数保留两位小数，排序并列时按当前实现的稳定排序规则处理。

### 2.6 不可计算规则

以下情况视为“该证券在该交易日该 lookback 下不可计算”:

- 历史长度不足
- `adj_close` 与 `close` 同时缺失
- `data_status == "unavailable"`

不可计算时:

- `derived_indicator_daily` 对应 `rps_50` / `rps_120` / `rps_250` 字段保留为 `null`
- chart detail 的 `indicator_history` 返回 `null` 值而非伪造连续曲线
- 前端必须把缺失视为显式缺口，而不是自动补线或插值

## 3. 正式信号与解释性信息边界

### 3.1 正式筛选信号

MVP 中正式参与筛选与回测的 RPS 相关信号只有:

- `rps_50`
- `rps_120`
- `rps_250`
- `best_rps_value = max(rps_50, rps_120, rps_250)`，仅在可计算值之间取最大
- `rps_threshold`
- `best_rps_value >= rps_threshold` 的通过判定

### 3.2 解释性历史序列

`indicator_history` 是解释性历史序列，但它仍然必须来自后端权威事实表，而不是前端生成。

其职责是:

- 帮助用户回看在某个时间窗内真实存储过的 RPS 历史
- 与当次 `screen_run` 的判定值对齐
- 暴露缺失、不可计算和数据稀疏状态

### 3.3 非正式信号

以下内容在当前定义版本中不属于正式筛选或回测信号:

- `翻红`
- 任意前端临时绘制的走势注释
- 任意未进入后端契约与测试的图形状态

如果后续要把这些概念升级为正式信号，必须先修改本契约、PRD、epics 和测试。

## 4. 契约映射

### 4.1 Derived Facts

权威字段位于 `derived_indicator_daily`:

- `rps_50`
- `rps_120`
- `rps_250`
- `fifty_two_week_high`
- `high_proximity_ratio`

其中 RPS 三字段受本契约直接约束。

### 4.2 Screen Results

`screen_runs` / `screen_run_results` 使用当日派生事实做判定，RPS 相关解释字段包括:

- `rps_50`
- `rps_120`
- `rps_250`
- `best_rps_value`
- `rps_threshold`
- `rps_condition_passed`

### 4.3 Stock Detail Payload

`apps/api/src/stockanalyse_api/services/chart_data.py` 当前输出的 RPS 相关字段分为两类:

权威判定值:

- `latest_indicator_snapshot`
- `rule_breakdown.rps_condition.*`

权威历史序列:

- `indicator_history[*].rps_50`
- `indicator_history[*].rps_120`
- `indicator_history[*].rps_250`

解释性展示约束:

- 前端可以高亮阈值
- 前端可以显示“最佳 RPS 达到阈值/低于阈值”
- 前端不可以生成额外权威曲线

### 4.4 Backtesting

`apps/api/src/stockanalyse_api/services/backtesting.py` 当前通过 `evaluate_indicator_snapshot` 消费 `DerivedIndicatorDaily`。

因此本契约要求:

- backtest 与 screen 使用相同的 `rps_50/120/250` 事实
- backtest 的 RPS 通过判定遵循与 screen 相同的阈值语义
- 如未来引入定义版本字段，应同时落到 screen 与 backtest 的运行上下文

## 5. 已符合项与待修正项

### 5.1 已符合项

- `factor_materialization.py` 已按当前契约使用横截面收益百分位逻辑物化 RPS
- `chart_data.py` 已返回真实 `indicator_history`
- `StockDetailCharts.tsx` 已移除前端伪造衰减曲线
- `backtesting.py` 已直接消费 `DerivedIndicatorDaily`，没有另起一套 RPS 算法

### 5.2 待后续故事修正项

- 当前运行记录尚未持久化 `definition version`
- UI 仍未把“正式筛选信号”和“解释性图形状态”彻底分层表达
- `翻红` 仍是开放概念，尚未纳入正式定义
- 旧 story 文档与现有实现说明仍存在中英混合和契约散落问题

## 6. 后续故事引用方式

`3.5` 必须引用本文件来定义:

- 哪些图形状态是解释性信息
- 哪些值才是正式判定值

`5.4` 必须引用本文件来验证:

- backtest 是否与 screening 使用同一 RPS 语义
- chart explainability 是否与 screening 的同日判定值一致

## 7. 测试清单

后续必须覆盖的测试包括:

- RPS 百分位逻辑在多证券同日场景下的定义一致性
- 历史长度不足时 `rps_*` 返回 `null`
- `indicator_history` 末尾值与 `screen_run.trade_date` 对齐
- screen 与 backtest 对相同派生事实给出一致的 RPS 通过判定
- 缺失历史不会被前端补成伪连续曲线
