# 故事 2.8: Parameterize RPS Windows and Minimum Satisfied-Line Count

状态: done

## 用户故事

作为用户，  
我希望能够从批准的 RPS 窗口集合中选择哪些窗口参与策略，并指定至少满足几条，  
以便筛选规则更贴近我真实的迭代方式，而不是被固定在 50/120/250 任一条通过。

## 验收标准

1. 假设用户在 screening configuration workflow 中定义 RPS 规则，当其编辑配置时，那么产品接受一个“批准窗口集合中的已选窗口列表”和一个“至少满足几条”的参数。
2. 假设用户保存配置时输入了不可能的组合，当后端校验时，那么系统拒绝例如“要求满足数大于选中窗口数”这类无效配置。
3. 假设用户执行一次 screen run，当 backend 评估策略时，那么它使用选中的批准 RPS 窗口与配置的最少满足数量，而不是固定“任一条 50/120/250 通过”。
4. 假设 screen run 或 backtest run 被回看，当参数集被展示时，那么这些 RPS 规则输入能够清楚显示出来。

## 任务 / 子任务

- [x] 扩展 strategy configuration 持久化模型。 (AC: 1, 2, 4)
  - [x] 为策略配置增加批准窗口列表与最少满足数量字段。
  - [x] 增加 migration，并为已有配置提供兼容默认值。
  - [x] 把批准窗口集合定义为后端常量，而不是前端硬编码唯一真相。
- [x] 更新 strategy configuration API 与校验。 (AC: 1, 2)
  - [x] 返回批准窗口集合、当前选中窗口、以及最少满足数量。
  - [x] 拒绝未批准窗口、空窗口集合、以及最少满足数量越界。
- [x] 更新 screening / backtesting 评估逻辑。 (AC: 3, 4)
  - [x] 让 `evaluate_indicator_snapshot` 基于选中窗口集合和最少满足数量判断 RPS 条件。
  - [x] 保持 `best_rps_value` 等 explainability 字段仍然可用。
  - [x] 让 screen/backtest 参数集输出包含这些新增规则字段。
- [x] 更新前端策略配置 UI。 (AC: 1, 2, 4)
  - [x] 把 RPS 窗口改为从批准集合里勾选。
  - [x] 增加“至少满足几条”输入或选择器。
  - [x] 结果与回显文案需要能看出当前使用的窗口集合和满足数量。
- [x] 补测试与验证。 (AC: 1, 2, 3, 4)
  - [x] 扩展 strategy configuration、screening、backtesting 相关测试。
  - [x] 运行受影响前端 lint。

## 开发备注

- 你已确认采用方案 2：`只能从批准窗口集合里选，但这个集合可以扩展`。因此本故事不实现“任意天数自由输入并动态算新 RPS”。
- 当前 derived facts 只有 `rps_50 / rps_120 / rps_250` 三列，因此这一轮的批准集合应先绑定到这三项，并通过统一映射表实现可扩展性。 [Source: apps/api/src/stockanalyse_api/domain/indicators/models.py]
- 当前 screening 逻辑仍是 `max(rps_50, rps_120, rps_250) >= rps_threshold`，本故事要把它改成“选中窗口中至少 N 条满足阈值”。 [Source: apps/api/src/stockanalyse_api/services/screening.py]
- backtesting 复用 `evaluate_indicator_snapshot`，因此如果不一起更新 backtesting，会导致 screen/backtest 语义分叉。 [Source: apps/api/src/stockanalyse_api/services/backtesting.py]
- 现有 strategy configuration 只有 `rps_threshold` 和 `high_proximity_threshold_pct`，所以必须做 schema 迁移，不能只在前端拼参数。 [Source: apps/api/src/stockanalyse_api/domain/screens/models.py, apps/api/src/stockanalyse_api/services/strategy_config.py]

## References

- Epic story definition: [epics.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/epics.md:496)
- Strategy config model: [models.py](/Users/adam/Documents/GitHub/stockAnalyse/apps/api/src/stockanalyse_api/domain/screens/models.py:1)
- Strategy config service: [strategy_config.py](/Users/adam/Documents/GitHub/stockAnalyse/apps/api/src/stockanalyse_api/services/strategy_config.py:1)
- Strategy config route: [strategy_config.py](/Users/adam/Documents/GitHub/stockAnalyse/apps/api/src/stockanalyse_api/api/routes/strategy_config.py:1)
- Screening evaluation: [screening.py](/Users/adam/Documents/GitHub/stockAnalyse/apps/api/src/stockanalyse_api/services/screening.py:1)
- Backtesting evaluation: [backtesting.py](/Users/adam/Documents/GitHub/stockAnalyse/apps/api/src/stockanalyse_api/services/backtesting.py:1)
