# 美股自动化回测与参数优化说明

## 当前实现范围

本功能在现有 `run_cup_handle_rps_backtest` 单次回测函数外增加一层自动化优化能力：

- 按参数空间生成稳定的参数组合。
- 为每个参数组合生成 `parameter_hash`，用于去重和复现。
- 批量执行杯柄 + RPS + 财务过滤 + 买卖点回测。
- 在单次优化 worker 内缓存交易日、RPS 候选、财务行、杯柄物化事件池和交易模拟结果，避免相近参数重复扫描全市场或重复读取未来 K 线。
- 杯柄物化事件池使用轻量事件对象、按日期升序二分查找，并按具体杯柄参数预过滤，减少逐日筛选时重复判断形态参数。
- 支持网格搜索和带随机种子的随机搜索；随机搜索用于在大参数空间里抽样执行。
- 支持固定止损和 RPS 跌破阈值退出；触发价使用复权收盘价，卖出价使用触发后的下一个有效复权开盘价。止盈不使用固定百分比，后续按 RPS 走弱和业绩变差扩展。
- 优化回测默认组合口径为单票权重 10%、最多 10 只持仓、不允许同票在持仓中重复开仓，暂不设置冷却期。
- 持久化优化任务、每个参数组合的结果、评分和排名。
- 支持查询任务状态、结果排行榜、取消运行中任务。
- 支持把任意参数组合保存为策略预设，并激活为当前市场的预设。

## 当前本地数据覆盖

截至 2026-05-04 本地库检查结果：

- 美股 instrument：4800
- 有行情数据：4800 / 4800
- 有衍生指标：4800 / 4800
- 最新行情日：2026-04-30
- 最新指标日：2026-04-30
- 最新日行情覆盖：4694 / 4800
- 最新日指标覆盖：4692 / 4800
- 有净利润财务数据：4628 / 4800
- 有 PE / PB：Yahoo 当前估值源本轮返回 429，库内直接 PE/PB 仍为 0，不作为历史回测依据。
- 有经营现金流与自由现金流：SEC companyfacts 已完成主覆盖，经营现金流/自由现金流可用于正式筛选。
- 有 SEC 估值输入：0027 迁移新增 `diluted_eps`、`stockholders_equity`、`weighted_average_diluted_shares`。这些字段需要重新跑一次 SEC 财务刷新才会回填。

因此行情和指标主覆盖已经可用于回测；涉及财务条件的优化应关注 `missing`、`insufficient_history`、`valuation_missing` 和 `cash_flow_missing` 诊断计数。PE/PB 不再依赖 Yahoo 当前估值回填，而是在筛选/回测时用“信号日价格 + 当时可见 SEC 财务输入”动态计算，避免把 2026 年的当前估值带入历史信号。

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
  "use_rps": [true],
  "rps_threshold": [85, 90, 95],
  "selected_rps_windows": [[50, 120], [120, 250], [50, 120, 250]],
  "min_rps_windows_passing": [1, 2],
  "use_cup_handle": [true],
  "cup_handle_params": [
    {"min_cup_depth_pct": 5, "max_cup_depth_pct": 55}
  ],
  "fundamental_growth_params": [
    {
      "enabled": true,
      "min_years": 3,
      "min_growth_count": 1,
      "min_yoy_growth_pct": "0",
      "require_positive_net_income": true,
      "reporting_lag_days": 120,
      "max_pe": null,
      "max_pb": null,
      "require_positive_operating_cash_flow": false,
      "require_positive_free_cash_flow": false,
      "min_operating_cash_flow_growth_count": null,
      "min_operating_cash_flow_yoy_growth_pct": "0"
    },
    {
      "enabled": true,
      "min_years": 3,
      "min_growth_count": 2,
      "min_yoy_growth_pct": "10",
      "require_positive_net_income": true,
      "reporting_lag_days": 120,
      "max_pe": null,
      "max_pb": null,
      "require_positive_operating_cash_flow": true,
      "require_positive_free_cash_flow": false,
      "min_operating_cash_flow_growth_count": 1,
      "min_operating_cash_flow_yoy_growth_pct": "0"
    }
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

`cup_handle_params` 是一个杯柄参数对象数组。Dashboard 不再使用固定“杯柄形态组合”，而是把欧奈尔杯柄相关参数拆成独立优化轴，再在前端展开为 `cup_handle_params`：

- 周期：整体最短/最长、杯身最短/最长、把手最短/最长、形态回看窗口。
- 形态深度：杯深最小/最大、把手回调最小/最大、把手/杯深最大比例、把手最低位置、把手高出右唇、右唇容差。
- 底部结构：杯底停留天数、杯底区域、杯底跨度、杯底两侧最小比例。
- 突破与前涨：突破回看、是否要求突破放量、突破量均线天数、突破量倍数、是否要求前置上涨、前置上涨回看、前置上涨最小比例。

`use_rps` 和 `use_cup_handle` 是参数优化的归因开关；财务过滤在美股参数优化中是必选条件，只能调整要求强弱，不能关闭。关闭 RPS 或 K 线形态时，前端和后端都会折叠对应参数轴，避免同一策略因无效参数形成重复组合。默认财务网格已经向质量过滤收紧：3 年净利润、至少 1/2 次增长、0%/5%/10% 同比门槛、正利润、120 天报告滞后、经营现金流为正，并默认测试 `PE≤40/60` 与 `PB≤8/15`。在 0027 新字段完成全量刷新前，启用 PE/PB 会让缺失估值输入的股票进入 `valuation_missing`。

`fundamental_growth_params.max_pe` 和 `fundamental_growth_params.max_pb` 是可选估值过滤。一旦设置，最新可见财年必须能计算出对应估值且不超过阈值，否则状态会记为 `valuation_missing` 或 `valuation_failed`。计算优先级：

- PE：优先用 `signal close / diluted_eps`；缺 EPS 时用 `signal close * weighted_average_diluted_shares / net_income`。
- PB：用 `signal close * weighted_average_diluted_shares / stockholders_equity`。
- 只有财报币种为 USD 时才动态计算；非 USD 财报暂不做 FX 换算，避免价格币种和财报币种混用。
- Yahoo 当前 `pe/pb` 只在 `source_as_of_date <= signal_date` 时作为兜底，因此历史回测不会使用未来才取得的当前估值。

`fundamental_growth_params.require_positive_operating_cash_flow`、`require_positive_free_cash_flow`、`min_operating_cash_flow_growth_count` 和 `min_operating_cash_flow_yoy_growth_pct` 是现金流质量过滤。启用后所需财年必须有经营现金流/自由现金流数据，否则状态会记为 `cash_flow_missing`。经营现金流增长次数用于测试“利润增长之外，现金流是否同步改善”。当前建议把经营现金流为正作为默认质量门槛，自由现金流为正作为可选加强轴，因为自由现金流会天然惩罚重资本开支公司。

PE/PB 的替代方案包括：直接使用数据商当前 PE/PB、接入 IBKR fundamentals、或用 SEC 输入自行计算。策略回测优先采用第三种，因为它可以按信号日重算，最容易控制未来函数。IBKR 或 Yahoo 的当前估值更适合当日筛选，不适合作为历史回测的唯一估值来源。补齐新增 SEC 估值输入可用：

```bash
PYTHONPATH=apps/api/src python3 -m stockanalyse_api.jobs.refresh_fundamentals \
  --provider sec_companyfacts_yahoo_fallback \
  --exchange US \
  --missing-valuation-inputs-only \
  --progress-every 25
```

2026-05-03 的 600 组财务必选样本显示，原先围绕传统欧奈尔窄区间的杯柄参数交集过小：600 组中 532 组没有完成交易，最多仅 3 笔交易，排名靠前结果重复命中同两笔交易。事件级拆解显示，周期+深度过滤后仍有 3394 个事件，但右唇容差、把手位置、把手深度占杯深、把手高出右唇等曲线约束会把事件压到 98 个，叠加前置上涨后只剩 32 个。因此默认杯柄参数网格已切换为“宽松候选池”：

- 入场 RPS 默认先测试 `80,85`，RPS 窗口默认保留 `50+120` 和 `50+120+250`，避免财务必选后与过高 RPS 形成过窄交集。
- 杯深扩展到 `5/8/10/12` 至 `33/45/55`，把手回调扩展到 `1/2` 至 `12/20/25`。
- 周期先固定为宽候选池：杯身 `35-330`、把手 `3-60`、整体 `80-420`、形态回看 `750`，避免周期轴把 600 条随机样本稀释掉。
- 曲线形态放宽右唇容差 `5/10/15`、把手最低位置 `40/55/66`、把手/杯深比例 `35/60/80`、把手高出右唇 `2/5/8`。
- 前置上涨改为 `false,true` 对照，并将最小涨幅先放宽到 `10,20`。
- 杯底约束先固定为较宽的 `35` 区域、停留 `2` 天、跨度 `5`，突破回看固定 `60`。下一轮样本量恢复后再单独收紧杯底结构。

基于 2026-05-03 手动跑出的 96 组结果，默认杯柄优化空间围绕唯一有成交的族展开：不开财务、不要求突破放量、杯深 12-33、把手回调 3-12、前置上涨 30%。下一轮杯柄参数基础样本建议使用 5 年训练区间、验证区间留空、随机搜索 600 组；默认完整空间约 972 组，随机 600 组可以先建立可复现的基础样本，并保存到 `optimization_runs` / `optimization_results` 供后续分析读取。

2026-05-04 后续运行已经把目标函数改为 `robust_annualized_return`。这个目标会惩罚训练/验证差距和回撤，因此排名不再追逐验证期单段暴涨。run #24 的 120 组完整样本里，稳健排名靠前的形态大致集中在：RPS 85/90、窗口 `50+120+250` 或 `50+120`、3 年净利润 1/2 次增长、杯深最小 10/12、杯深最大 45、较浅右唇容差和把手位置约 55%。run #26 的 114 组样本进一步显示，RPS 90、杯深最大 45、前置上涨 true、把手高出右唇 2/5 更常出现在前 30 名。止损 6%/8%/10% 和 RPS 退出 75/80/85 仍未明显收敛。

下一轮基础样本不建议继续扩大到几十万组合随机抽样，而应做小规模配对实验：固定 run #24/#26 已验证的杯柄族，只对 RPS 阈值、净利润严格度、止损、RPS 退出、持仓上限和现金流质量做局部网格。这样能减少随机样本方差，也更接近“为未来实盘稳健性优化”，而不是拟合旧数据。

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
- `objective`: 排序目标，支持 `score`、`average_annualized_return`、`robust_annualized_return`、`annualized_return`、`max_drawdown`、`return_drawdown_ratio`、`win_rate`、`total_return`
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
- 财务筛选会返回明确状态：`missing` 表示没有可用财务数据，`insufficient_history` 表示年份不足，`not_positive` 表示正利润条件不满足，`valuation_missing` 表示启用 PE/PB 但估值缺失，`valuation_failed` 表示 PE/PB 超过阈值，`cash_flow_missing` 表示启用现金流过滤但数据缺失，`cash_flow_not_positive` 表示现金流非正，`cash_flow_growth_failed` 表示经营现金流增长次数不满足，`growth_failed` 表示净利润增长次数不满足，`passed` 表示通过。`not_required` 只用于旧结果或手动关闭财务的非优化路径。
- 支持设置训练区间和验证区间。
- 支持编辑第一版小网格参数：RPS/K线形态启用开关、RPS 阈值、RPS 窗口组合、财务质量门槛、持有天数、止损比例、RPS 退出阈值、持仓上限、单票权重、进场等待窗口。财务在参数优化中必选，只能调弱或调强，不能关闭。
- 支持编辑买点延迟日；回测会在信号后延迟指定交易日，再在等待窗口内使用首个有效开盘价进场。
- 支持逐项编辑杯柄参数优化轴，覆盖周期、杯深、把手、杯底、前置上涨和突破放量参数。
- 支持选择排序目标：综合得分、平均年化收益、稳健年化收益、最高年化收益、最低回撤、最高收益回撤比、最高胜率、最高总收益。
- `robust_annualized_return` 用训练期和验证期的较低年化收益作为核心，同时惩罚训练/验证差距、最大回撤和样本不足；用于降低“验证期暴涨但训练期无效”的参数被排到前面的概率。
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
- 评估是否需要跨任务持久化缓存；当前只做单次优化 worker 内缓存，避免数据过期问题。
- 贝叶斯优化暂缓；先用可复现随机搜索积累结果，等确认参数空间和评分稳定后再评估是否引入更复杂优化器。
