# 杯柄候选池物化设计方案

## 背景

当前杯柄回测的性能瓶颈不在交易模拟，而在每个交易日重复执行全市场筛选和形态检测：

- 对每个交易日先筛 RPS 候选股。
- 对每个候选股加载约 520 日历史 K 线。
- 在历史窗口内重新扫描杯柄结构。
- 参数优化会对不同杯柄参数重复上述过程。

实测美股一年区间下，杯柄关闭的单次回测约 50 秒；杯柄开启后单个交易日筛选约 27 秒，完整一年会进入小时级。这个性能形态说明需要把杯柄形态扫描从回测主路径移出。

核心方向是：不要物化某一套杯柄参数的 `passed=true/false`，而是物化一套足够宽松的杯柄候选事件和形态特征。后续参数优化只在候选事件表上做范围过滤。

## 目标

- 支持杯柄参数尚未确定时仍可高效优化。
- 避免每次回测重复加载 K 线和扫描形态。
- 允许不同杯深、柄深、周期、前期上涨、突破放量参数在同一候选池上快速比较。
- 对候选池覆盖不了的参数明确提示，而不是静默走错结果。
- 保持结果可复现：每次回测知道使用了哪个候选池版本、生成边界和数据水位。

## 非目标

- 不在第一版引入复杂机器学习或贝叶斯优化。
- 不把所有可能的杯柄解释都无限制存入数据库。
- 不承诺任意参数都能从同一候选池中恢复；超出覆盖范围时需要重建候选池或显式回退慢路径。

## 总体方案

新增一层杯柄候选池：

1. 使用宽松上界离线扫描行情数据，生成 `cup_handle_pattern_events`。
2. 每条事件代表一次真实突破形态，而不是某个参数组合的最终通过结果。
3. 事件保存完整结构特征，例如杯深、柄深、周期、前期上涨幅度、突破量能倍数。
4. 回测或优化时，把 `cup_handle_params` 翻译成 SQL 过滤条件。
5. 只有当参数空间落在候选池覆盖范围内时，才使用物化快路径。

## 推荐的宽松候选池参数

第一版候选池应覆盖比当前默认参数更宽的范围，但仍保留基本结构约束，避免候选表膨胀到不可用。

| 参数 | 当前默认 | 候选池生成边界 | 说明 |
| --- | ---: | ---: | --- |
| `lookback_days` | 520 | 750 | 覆盖最长 420 日总形态、180 日前期上涨和 60 日突破回看。 |
| `min_cup_duration` | 60 | 35 | 捕捉较短成长股整理。 |
| `max_cup_duration` | 220 | 330 | 覆盖更长底部和大级别整理，并与 420 日总周期上界匹配。 |
| `min_handle_duration` | 5 | 3 | 允许短把手。 |
| `max_handle_duration` | 40 | 90 | 允许长把手或平台式把手。 |
| `min_total_duration` | 120 | 50 | 不把短杯柄提前排除。 |
| `max_total_duration` | 260 | 420 | 与杯身、把手宽松上界匹配。 |
| `min_cup_depth_pct` | 12 | 5 | 捕捉浅杯和高位平台。 |
| `max_cup_depth_pct` | 33 | 60 | 覆盖高波动成长股和熊市修复杯。 |
| `min_handle_pullback_pct` | 3 | 1 | 捕捉极浅把手。 |
| `max_handle_pullback_pct` | 12 | 35 | 允许更深把手，后续参数过滤再收紧。 |
| `max_right_lip_delta_pct` | 5 | 15 | 宽松保留左右杯沿不完全对称的形态。 |
| `require_prior_uptrend` | true | false | 生成阶段不强制；存储多个前期上涨窗口供过滤。 |
| `prior_uptrend_lookback_days` | 120 | 60/90/120/180 | 存储多窗口特征；优化阶段选择窗口。 |
| `min_prior_uptrend_pct` | 30 | 不限制 | 存为数值，由参数过滤。 |
| `min_handle_low_position_pct` | 66 | 40 | 保留中上部把手，最终阈值可优化。 |
| `max_handle_depth_to_cup_depth_pct` | 35 | 80 | 宽松保留深把手，最终阈值可优化。 |
| `max_handle_high_above_lip_pct` | 2 | 8 | 允许把手轻微高出右杯沿。 |
| `min_bottom_dwell_days` | 5 | 2 | 生成阶段只排除尖锐 V 型极端噪声。 |
| `bottom_zone_pct` | 20 | 35 | 用更宽杯底区域计算候选；另存多阈值特征。 |
| `min_bottom_span_pct` | 10 | 5 | 避免过早排除较紧凑杯底。 |
| `min_cup_side_duration_pct` | 20 | 10 | 保证杯底不贴边，但比默认更宽松。 |
| `require_breakout_volume` | false | false | 生成阶段不强制；存储多窗口量能倍数。 |
| `breakout_volume_avg_days` | 50 | 20/50/60 | 存储多窗口特征；优化阶段选择窗口。 |
| `min_breakout_volume_multiplier` | 1.4 | 不限制 | 存为数值，由参数过滤。 |
| `breakout_lookback_days` | 30 | 60 | 查询阶段用事件 `breakout_date` 支持 1-60 日回看。 |

这个候选池不是最终策略判断。它只是保证“可能像杯柄”的结构不会被提前丢掉。第一轮优化建议把用户可调范围限制在上述覆盖范围内。

## 需要物化的事件字段

建议新增事件表 `cup_handle_pattern_events`，核心字段如下：

```text
id
market
instrument_id
symbol_snapshot
breakout_date
left_lip_date
cup_bottom_date
right_lip_date
handle_low_date
cup_duration
handle_duration
total_duration
cup_depth_pct
handle_depth_pct
right_lip_delta_pct
handle_low_position_pct
handle_depth_to_cup_depth_pct
handle_high_above_lip_pct
bottom_dwell_days_zone_20
bottom_dwell_days_zone_35
bottom_span_pct_zone_20
bottom_span_pct_zone_35
left_side_duration_pct
right_side_duration_pct
prior_uptrend_pct_60
prior_uptrend_pct_90
prior_uptrend_pct_120
prior_uptrend_pct_180
breakout_volume_ratio_20
breakout_volume_ratio_50
breakout_volume_ratio_60
breakout_close_over_resistance_pct
data_start_date
data_end_date
detector_version
materialization_run_id
created_at
```

推荐索引：

```text
(market, breakout_date)
(instrument_id, breakout_date)
(materialization_run_id)
(market, breakout_date, instrument_id)
```

如果 SQLite 查询仍慢，再评估对常用数值列增加组合索引；第一版优先控制表规模和查询路径清晰。

## 候选池运行元数据

建议新增 `cup_handle_materialization_runs`：

```text
id
market
status
started_at
completed_at
source_start_date
source_end_date
latest_market_data_date
generation_bounds_json
feature_windows_json
detector_version
events_created
symbols_processed
error_message
```

每次回测结果或优化任务需要记录使用的 `materialization_run_id`。这样以后行情修正、检测器调整或候选池重建时，历史结果不会被静默改写。

## 参数过滤映射

优化时把 `cup_handle_params` 转成事件表过滤：

```sql
event.cup_duration BETWEEN :min_cup_duration AND :max_cup_duration
AND event.handle_duration BETWEEN :min_handle_duration AND :max_handle_duration
AND event.total_duration BETWEEN :min_total_duration AND :max_total_duration
AND event.cup_depth_pct BETWEEN :min_cup_depth_pct AND :max_cup_depth_pct
AND event.handle_depth_pct BETWEEN :min_handle_pullback_pct AND :max_handle_pullback_pct
AND event.right_lip_delta_pct <= :max_right_lip_delta_pct
AND event.handle_low_position_pct >= :min_handle_low_position_pct
AND event.handle_depth_to_cup_depth_pct <= :max_handle_depth_to_cup_depth_pct
AND event.handle_high_above_lip_pct <= :max_handle_high_above_lip_pct
AND event.breakout_date BETWEEN :signal_date_minus_breakout_lookback AND :signal_date
```

前期上涨：

- 如果 `require_prior_uptrend=false`，不加前期上涨过滤。
- 如果 `require_prior_uptrend=true`，使用对应窗口列，例如 `prior_uptrend_pct_120 >= :min_prior_uptrend_pct`。
- 第一版建议只允许 `prior_uptrend_lookback_days` 为 `60/90/120/180`。如果用户需要任意窗口，应先重建候选池并增加对应特征列。

突破量：

- 如果 `require_breakout_volume=false`，不加量能过滤。
- 如果 `require_breakout_volume=true`，使用对应窗口列，例如 `breakout_volume_ratio_50 >= :min_breakout_volume_multiplier`。
- 第一版建议只允许 `breakout_volume_avg_days` 为 `20/50/60`。

杯底圆弧参数：

- `bottom_zone_pct`、`min_bottom_dwell_days`、`min_bottom_span_pct` 不适合作为单一字段任意优化，因为不同 `bottom_zone_pct` 会改变杯底区域计算。
- 第一版建议固定生成时使用宽松圆弧约束，同时物化 `zone_20` 和 `zone_35` 两组杯底特征。
- 如果后续要把杯底参数纳入大范围优化，需要为候选池增加多阈值特征，或把这类参数列为“变更后需要重建候选池”的参数。

## 回测消费方式

现有回测逐日调用 `screen_universe`。改造后可以保持接口不变，但内部路径分为两种：

1. `use_cup_handle=false`：沿用当前 RPS/财务筛选。
2. `use_cup_handle=true` 且参数被候选池覆盖：走物化事件查询。
3. `use_cup_handle=true` 但参数超出候选池覆盖：拒绝启动并提示重建候选池，或显式选择慢路径。

推荐默认行为是拒绝启动并提示重建，不建议静默慢路径。静默慢路径会让用户误以为任务卡住。

查询顺序建议：

1. 先按交易日、市场和 RPS 条件筛出候选股票。
2. 再对这些 `instrument_id` 查询匹配的杯柄事件。
3. 若同一股票在回看窗口内有多个事件，默认选择距离 `signal_date` 最近的 `breakout_date`。
4. 回测买点可以继续使用当前 `signal_date` 逻辑；后续可新增“只在实际突破日买入”的模式，减少同一突破事件在回看窗口内重复触发。

## 物化任务实现建议

不要按“交易日 x 候选股”重复扫描。物化任务应按股票执行：

1. 按市场读取股票列表。
2. 每只股票一次性加载完整历史 K 线。
3. 在单只股票序列内逐个 `breakout_idx` 扫描候选事件。
4. 为每个真实突破日生成最多一组或少量最优候选事件。
5. 写入事件表，记录 `materialization_run_id`。

第一版可以复用现有检测器的结构判断，但需要把“检测最近窗口”改成“检测指定突破日”。这样避免对同一股票、相邻交易日反复加载和反复扫描同一段历史。

增量更新：

- 每天数据更新后，只处理最新交易日附近可能产生新突破的股票。
- 每只股票仍需要加载最多 `lookback_days` 根 K 线，但只检查新增交易日作为 `breakout_idx`。
- 如果检测器版本或生成边界变化，触发全量重建。

## 覆盖范围校验

每个候选池必须暴露 `generation_bounds_json`。创建优化任务时先校验参数空间：

- `covered`：所有杯柄参数都在候选池边界和特征窗口内，允许快路径。
- `needs_rebuild`：参数超出边界，例如 `max_total_duration=500` 或 `breakout_volume_avg_days=100`。
- `unsupported`：参数无法从当前事件特征表达，例如任意 `bottom_zone_pct` 优化。

Dashboard 应显示当前候选池覆盖：

```text
杯柄候选池：US，行情截至 2026-04-30，覆盖总周期 50-420 日，杯深 5%-60%，突破回看 1-60 日。
```

如果参数不覆盖，按钮旁显示：

```text
当前杯柄候选池不覆盖 max_total_duration=500。请重建候选池或缩小参数范围。
```

## 推荐的第一版实施步骤

1. 建表和元数据
   - 新增 `cup_handle_materialization_runs`。
   - 新增 `cup_handle_pattern_events`。
   - 为优化任务记录 `cup_handle_materialization_run_id`。

2. 重构检测器入口
   - 保留现有 `_detect_cup_handle_pattern` 给图表和兼容路径使用。
   - 新增“按指定突破日检测”的内部函数。
   - 物化任务按股票一次性扫描，避免逐日重复加载。

3. 生成历史候选池
   - 先只支持 `market=us`。
   - 使用本文推荐宽松参数。
   - 输出候选数量、覆盖日期、耗时、失败股票列表。

4. 接入筛选和回测
   - `screen_universe` 在杯柄开启时优先查事件表。
   - 参数不覆盖时返回明确错误。
   - 优化任务记录所用候选池版本。

5. Dashboard 展示
   - 增加候选池状态。
   - 增加“重建杯柄候选池”维护入口。
   - 在参数优化启动前做覆盖校验。

6. 性能验收
   - 单个交易日杯柄筛选应从 20 秒级降到 1 秒内。
   - 一年单参数回测应从小时级降到分钟级以内。
   - 参数优化任务应能展示稳定进度，而不是长时间停留在第 1 组。

## 当前实现入口

第一版后端实现提供了一个命令行物化入口：

```bash
cd apps/api
STOCKANALYSE_DB_PATH=/Users/adam/Documents/GitHub/stockAnalyse/data/stockanalyse.db \
PYTHONPATH=src \
python3 -m stockanalyse_api.jobs.materialize_cup_handle_candidates --market us
```

可选参数：

```bash
--source-start-date 2020-01-01
--source-end-date 2026-04-30
--commit-every 100
```

回测和筛选路径会优先查 `cup_handle_pattern_events`。如果当前数据库尚未迁移、没有完成的候选池，或参数超出候选池覆盖范围，系统会回退到原有运行时扫描路径，以保持现有 Dashboard 行为不被阻断。

## 风险和取舍

- 候选池越宽，误候选越多，查询表越大；候选池越窄，未来调参越容易漏掉形态。
- 前期上涨和突破放量适合存多窗口特征；杯底圆弧这类依赖阈值重新划分区域的参数，不适合无限制优化。
- 如果把实际突破日事件用于回测，可能与当前“突破回看 N 日内仍算通过”的筛选语义产生差异；第一版应保留回看窗口过滤，后续再引入严格突破日买点模式。
- 物化结果必须带检测器版本和数据水位，否则历史优化结果难以解释。

## 推荐默认策略

第一版采用“宽松候选池 + 覆盖范围校验 + SQL 快速过滤”：

- 候选池生成使用本文推荐宽松边界。
- 可优化参数优先放在可直接过滤的结构指标上：周期、杯深、柄深、唇差、把手位置、前期上涨阈值、突破量倍数。
- 对任意杯底圆弧参数优化暂不开放，只保留少量预计算阈值。
- 参数超出候选池时不静默慢跑，而是提示重建候选池。

这样不会把策略锁死在当前默认杯柄参数上，同时能把最重的 K 线扫描从回测主路径移走。
