# 美股自动化回测与参数优化说明

## 当前实现范围

本功能在现有 `run_cup_handle_rps_backtest` 单次回测函数外增加一层自动化优化能力：

- 按参数空间生成稳定的参数组合。
- 为每个参数组合生成 `parameter_hash`，用于去重和复现。
- 批量执行杯柄 + RPS + 财务过滤 + 买卖点回测。
- 在单次优化任务进程内缓存相同交易日和相同筛选参数的候选股票结果，避免卖点参数变化时重复扫描全市场。
- 支持网格搜索和带随机种子的随机搜索；随机搜索用于在大参数空间里抽样执行。
- 支持固定止损和 RPS 跌破阈值退出；触发价使用复权收盘价，卖出价使用触发后的下一个有效复权开盘价。止盈不使用固定百分比，后续按 RPS 走弱和业绩变差扩展。
- 优化回测默认组合口径为单票权重 10%、最多 10 只持仓、不允许同票在持仓中重复开仓，暂不设置冷却期。
- 持久化优化任务、每个参数组合的结果、评分和排名。
- 支持查询任务状态、结果排行榜、取消运行中任务。
- 支持把任意参数组合保存为策略预设，并激活为当前市场的预设。

## 当前本地数据覆盖

截至 2026-05-01 本地库检查结果：

- 美股 instrument：4800
- 有行情数据：4800 / 4800
- 有衍生指标：4800 / 4800
- 最新行情日：2026-04-30
- 最新指标日：2026-04-30
- 最新日行情覆盖：4694 / 4800
- 最新日指标覆盖：4692 / 4800
- 有净利润财务数据：4628 / 4800

因此行情和指标主覆盖已经可用于回测，但财务数据仍有约 172 只缺口；涉及财务条件的优化应关注 `missing` 和 `insufficient_history` 诊断计数。

## 数据表

- `optimization_runs`：一次参数优化任务。
- `optimization_results`：某个优化任务下的单个参数组合结果。
- `strategy_presets`：可复用的命名策略参数预设。

每个优化任务会保存：

- 市场：当前目标是 `us`。
- 训练区间：`train_start_date` / `train_end_date`。
- 验证区间：可选的 `validation_start_date` / `validation_end_date`。
- 原始参数空间 JSON。
- 展开后的参数组合 JSON。
- 数据快照：来自 dashboard overview，包括行情、指标和财务覆盖状态。
- 进度：总参数数、完成数、失败数、当前状态、最佳结果 ID。

## 参数空间格式

示例：

```json
{
  "rps_threshold": [85, 90, 95],
  "selected_rps_windows": [[50, 120], [120, 250], [50, 120, 250]],
  "min_rps_windows_passing": [1, 2],
  "fundamental_growth_params": [
    {"enabled": false},
    {"enabled": true, "min_years": 3, "min_growth_count": 2, "require_positive_net_income": true}
  ],
  "holding_days": [60, 100, 130],
  "stop_loss_pct": ["-0.06", "-0.08", "-0.10"],
  "take_profit_pct": [null],
  "rps_exit_threshold": [80, 85],
  "portfolio_cap": [10],
  "position_weight_pct": ["0.10"],
  "allow_reentry_while_open": [false],
  "entry_delay_days": [0, 1, 2],
  "entry_deferral_window_days": [5]
}
```

`cup_handle_params` 可以只传需要覆盖的字段，未传字段会使用当前默认杯柄参数。Dashboard 里的“加入杯柄形态组合”会在当前杯柄基础参数上展开 16 组第二阶段形态组合：

- 杯深：12%-35%、15%-40%
- 柄深：5%-15%、8%-20%
- 突破量能：关闭、开启且要求 1.5 倍
- 前期上涨：关闭、开启且要求 20%+

启用该选项会直接放大总组合数，应结合“最大组合数”限制使用，避免一次性启动过大的笛卡尔积。

## API

创建优化任务：

```http
POST /backtests/optimization/runs
```

请求体核心字段：

- `market`: 默认 `us`
- `train_start_date`
- `train_end_date`
- `validation_start_date`
- `validation_end_date`
- `parameter_space`
- `objective`: 排序目标，支持 `score`、`average_annualized_return`、`annualized_return`、`max_drawdown`、`return_drawdown_ratio`、`win_rate`、`total_return`
- `search_mode`: `grid` 或 `random`；随机搜索会从完整参数空间中抽样，避免大网格一次性全跑
- `random_seed`: 随机搜索种子，用于复现实验
- `max_parameter_sets`
- `max_workers`: 可选；留空或 `null` 表示按机器 CPU 自动选择，多进程并行评估参数集
- `execute_immediately`

查询任务：

```http
GET /backtests/optimization/runs/{id}
```

查询结果排行榜：

```http
GET /backtests/optimization/runs/{id}/results?limit=100&offset=0
```

取消任务：

```http
POST /backtests/optimization/runs/{id}/cancel
```

保存策略预设：

```http
POST /strategy-presets
```

查看、编辑、复制、删除策略预设：

```http
GET /strategy-presets?market=us
GET /strategy-presets/{id}
PATCH /strategy-presets/{id}
POST /strategy-presets/{id}/duplicate
DELETE /strategy-presets/{id}
```

激活策略预设：

```http
POST /strategy-presets/{id}/activate
```

## Dashboard 入口

Dashboard 的“美股参数优化”卡片提供第一版可操作流程：

- 自动使用当前美股数据覆盖概览作为运行前数据预检参考。
- 财务筛选会返回明确状态：`not_required` 表示财务条件关闭，`missing` 表示没有可用财务数据，`insufficient_history` 表示年份不足，`not_positive` 表示正利润条件不满足，`growth_failed` 表示增长次数不满足，`passed` 表示通过。
- 支持设置训练区间和验证区间。
- 支持编辑第一版小网格参数：RPS 阈值、RPS 窗口组合、财务条件开关、持有天数、止损比例、RPS 退出阈值、持仓上限、单票权重、进场等待窗口。
- 支持编辑买点延迟日；回测会在信号后延迟指定交易日，再在等待窗口内使用首个有效开盘价进场。
- 支持可选展开第二阶段杯柄形态组合，覆盖杯深、柄深、突破量能和前期上涨要求。
- 支持选择排序目标：综合得分、平均年化收益、最高年化收益、最低回撤、最高收益回撤比、最高胜率、最高总收益。
- 支持网格搜索和可复现随机搜索；随机搜索时“最大组合数”表示抽样数量。
- 前端会预估参数组合数量，并用 `最大组合数` 阻止过大的网格直接启动。
- 运行中显示任务状态、完成数量、失败数量、总参数数量、进度条和当前最佳结果 ID。
- 结果表展示排行榜、得分、核心参数、验证指标和保存预设按钮。
- 结果表支持勾选多组参数进行对比，显示权益曲线、年度收益、总收益、年化收益、最大回撤、止损比例和连续亏损次数。
- 结果表和对比表会显示训练期排名、验证期排名和训练/验证排名差，用于观察参数是否存在过拟合迹象。
- 保存预设会写入 `strategy_presets`，并记录来源优化任务 ID 和结果 ID。
- 优化结果支持“重跑”，会把该参数应用到当前回测表单，并使用训练开始日至验证结束日重新跑完整区间。
- 策略预设列表支持应用、设为默认、复制、改名和删除；默认预设会在 Dashboard 加载当前市场时自动应用。

## 评分逻辑

当前第一版评分是基础综合分，用于生成排行榜。若提供验证区间，则优先使用验证区间指标评分：

```text
score =
  平均单笔收益
  + 年化收益 * 0.15
  + 胜率 * 0.10
  + min(收益回撤比 * 0.02, 0.10)
  + 样本数奖励
  - 最差单笔绝对值
  - 最大回撤绝对值 * 0.15
  - 止损触发比例 * 0.05
  - 连续亏损惩罚
  - 样本不足惩罚
```

样本不足惩罚：

- 少于 10 笔完成交易：惩罚 0.20
- 少于 50 笔完成交易：惩罚 0.08
- 50 笔及以上：无惩罚

总收益、年化收益、平均年化收益、最大回撤、收益回撤比、权益曲线和年度收益来自回测结果里的信号日平均收益序列，并按单票权重进行暴露近似。止损、RPS 退出触发比例和最大连续亏损次数来自完整交易序列，不受 Dashboard 返回交易条数限制。这个口径是第一版组合级近似指标，用于调参排序；后续会用完整逐日权益曲线替换。

优化任务完成排名时会同时写入：

- `train_metrics.train_rank`：按当前排序目标计算的训练期排名。
- `validation_metrics.validation_rank`：按当前排序目标计算的验证期排名。
- `validation_metrics.train_validation_rank_gap`：验证期排名减训练期排名，正数表示验证期名次变差，负数表示验证期名次改善。

## 自动化验收

- 测试覆盖参数 hash 稳定性、参数网格展开、结果排序、失败隔离、目标函数切换、样本不足惩罚、风险指标提取、筛选缓存和策略预设 CRUD。
- 批量优化验收覆盖 33 组参数组合，并验证其中一个组合失败时任务仍会继续完成、持久化失败结果并完成排名。

## 后续优化方向

- 接入市场基准年度收益后，按自然年拆分牛市、熊市、震荡市表现。
- 增加 walk-forward 多窗口验证，降低过拟合风险。
- 继续增强 Dashboard 杯柄形态多区间参数编辑界面，后续可把杯持续天数、柄持续天数和突破回看天数也纳入网格。
- 评估是否需要跨任务持久化缓存；当前只做单次优化任务进程内缓存，避免数据过期问题。
- 贝叶斯优化暂缓；先用可复现随机搜索积累结果，等确认参数空间和评分稳定后再评估是否引入更复杂优化器。
