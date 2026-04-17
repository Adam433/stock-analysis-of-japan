# Portfolio-Return Backtest Anchor

状态: approved semantic source for Epic 5 v3 patch

## 目的

本文件是 portfolio-return backtest 语义的唯一 anchor。任何未来涉及 backtest 执行、sizing、holding、stop-loss、排名裁剪、结果聚合或 lifecycle 区分的实现、测试、文档与 story，都必须显式引用本文件；除非在对应 story 中明文声明偏离，否则一律不得默默回退到 historical condition-hit 统计模型。

## Normative Sources

- FR45：win-rate 与 maximum drawdown 的规范定义来自 `_bmad-output/planning-artifacts/prd.md`，本 anchor 不重述定义，只引用该规范源。
- FR68 / FR70 / FR71 / FR73：入场 deferral、平仓 deferral、默认 entry deferral window、以及 `backtest_lifecycle` 语义来源于 PRD 与 Epic 5 requirements inventory。
- Stories 5.4 / 5.5：策略定义不在 backtest record 上重复持久化，而是通过 `screen_run_id` 引用解析。

## Locked MVP Semantics

### 1. 入场

- 入场时点固定为 T+1 open。
- 入场沿用 FR68 的 deferral 与 exclusion 规则。
- `entry_deferral_window_days` 为可配置参数。
- MVP 默认值为 5 trading days。
- 若在 deferral window 内找不到有效入场交易日，则该标的不入场，不进行补录。

### 2. 仓位与初始组合值

- 仓位采用 equal-weight sizing。
- `portfolio_cap` 为可配置参数。
- MVP 默认 `portfolio_cap = 20` securities。
- 采用 fractional share sizing 作为 MVP 简化。
- 初始 `portfolio_value` 定义为无量纲常量 `1.0`。
- 若本次实际入场证券数为 `N`，则每只证券初始权重恒为 `1 / N`。
- 所有 portfolio-level return 都以权重比率表示，不以货币金额表示。

### 3. Cap Exclusion 排名策略

- 当可入场证券数超过 `portfolio_cap` 时，使用单一确定性排名策略裁剪。
- 排名字段为 RPS composite score，按降序排序。
- 若 composite score 相同，使用 ticker 作为确定性 tie-breaker。
- MVP 不引入人工裁量或随机性。

### 4. 持有期

- 持有期为可配置参数，单位为 trading days。
- MVP 默认持有期为 20 trading days。
- 持有期从实际入场成交日开始计算。

### 5. 单证券 Stop-Loss

- stop-loss 为单证券级别参数，对照该证券自身入场价计算。
- 阈值可配置。
- MVP 默认值为 `-8%`。
- breach 信号每天仅计算一次。
- breach 检查价格使用 daily adjusted close。

### 6. 平仓

- stop-loss 触发后的平仓执行时点为 next valid trading day open。
- 持有期到期后的平仓执行时点同样为 next valid trading day open。
- 平仓沿用 FR70 的 deferral 规则。
- 若 deferral 规则最终找不到有效平仓交易日，则该仓位按 FR70 处理，不允许私自创造额外语义。

### 7. 组合行为边界

- 单次 backtest 内不允许 rebalance。
- 单次 backtest 内不允许 re-entry。
- 单次 backtest 内不允许对已持仓加仓。
- 已释放的现金在同一次 backtest 内不再投入。
- MVP 语义是“一次性建仓后被动持有/止损退出”，不是持续滚动调仓模型。

### 8. 指标与 Benchmark 边界

- win-rate 与 maximum drawdown 的定义直接引用 FR45。
- 本 anchor 不重新定义 win-rate。
- 本 anchor 不重新定义 maximum drawdown。
- MVP 不包含 benchmark 比较。

### 9. 每次 Run 必须持久化的上下文

每次 run 必须持久化以下内容：

- source `screen_run_id`
- parameter snapshot
- holding parameters
- stop-loss parameters
- portfolio cap
- `entry_deferral_window_days`
- ranking policy identifier
- dataset-version identifier
- effective default values
- `backtest_lifecycle`

其中：

- `backtest_lifecycle` 默认值为 `portfolio_return`
- legacy historical runs 必须标记为 `legacy_condition_hit`

### 10. 策略定义的 provenance 边界

- backtest record 不单独再存 RPS contract version。
- backtest record 不单独再存 screening parameters。
- 这些策略定义只通过 `screen_run_id` 引用解析。
- Story 5.4 与 Story 5.5 是该 provenance 语义的直接来源；本 anchor 与其保持一致。

### 11. Lifecycle Segregation Contract

- `portfolio_return` 与 `legacy_condition_hit` 是两种不同 lifecycle。
- result list、comparison surface、portfolio-level aggregation surface 不得把 `legacy_condition_hit` runs 混入 `portfolio_return` 统计。
- legacy runs 可以展示、检索、追溯，但必须有显式视觉标签，且与 portfolio-return runs 可视化区隔。
- 任何未来新增的 result-list、comparison、summary、dashboard、analytics 组件，都必须先按 `backtest_lifecycle` 做过滤或分组，再进行 portfolio-return 指标展示。

### 12. Forbidden Regression

以下行为都视为违反本 anchor：

- 把 backtest 结果解释回 “historical condition-hit qualifying observation count” 模型，却未明文声明偏离
- 在组合收益统计里混入 legacy condition-hit runs
- 在 backtest record 上重新复制存储应由 `screen_run_id` 解析的策略定义
- 在未声明的情况下引入 rebalance、re-entry、加仓、现金再投入或 benchmark 逻辑

## Future Story 引用契约

- 任何未来涉及 backtest 执行、sizing、holding、stop-loss 行为的 story，必须显式引用本 anchor。
- 任何偏离本 anchor 的地方，必须在 story 的验收标准或开发备注中显式声明。
- 不允许通过实现细节、测试命名、默认值漂移或 UI 文案，暗中回退到 condition-hit 模型。
- 任何展示 portfolio-return 组合层指标的路径，都必须显式过滤 `backtest_lifecycle === 'portfolio_return'`。

## Stories 5.1-5.5 语义反向追溯

- Story 5.1 `launch-portfolio-return-backtest-from-screen-run`：创建 run 时依赖本 anchor 的 lifecycle、持久化上下文与 provenance 边界。
- Story 5.2 `execute-portfolio-return-backtest-with-entry-holding-stop-loss`：执行引擎直接依赖本 anchor 锁定的 entry、sizing、holding、stop-loss、cash handling 语义。
- Story 5.3 `review-portfolio-return-backtest-results-and-compare`：结果展示与对比必须依赖本 anchor 的 lifecycle segregation、FR45 指标引用与 benchmark 边界。
- Story 5.4 `verify-backtest-alignment-via-source-screen-run`：语义一致性校验依赖本 anchor 的 `screen_run_id` provenance 约束。
- Story 5.5 `reference-screen-run-provenance-without-restoring-strategy`：provenance 解析规则直接受本 anchor 的“只通过 `screen_run_id` 引用解析”约束。

## Implementation Note

Story 5.6 仅负责锚定语义并通过 `backtest_lifecycle` 字段把 legacy 与 portfolio-return 生命周期区分开；它不实现 portfolio-return 执行引擎本身。
