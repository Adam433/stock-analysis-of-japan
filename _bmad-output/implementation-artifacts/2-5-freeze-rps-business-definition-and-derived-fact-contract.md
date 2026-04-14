# 故事 2.5: 冻结 RPS 业务定义与派生事实契约

状态: done

## 用户故事

作为产品与工程团队，  
我希望明确冻结 RPS 的业务定义、排名语义与派生事实契约，  
以便后续的筛选、图表展示与回测都不会偏离同一套预期方法。

## 验收标准

1. 假设团队正在纠正当前 MVP 的 RPS 方案，当本故事完成时，那么项目文档中会明确写出 RPS 50 / 120 / 250 的计算公式、排名宇宙、价格口径与不可计算数据的处理规则。
2. 假设已批准的 RPS 语义存在，当派生事实、筛选结果、股票详情载荷或回测逻辑消费 RPS 数据时，那么它们都引用同一份文档化契约，且任何前端近似或示意曲线都不会被视为权威数据。
3. 假设当前实现中已存在 RPS 派生事实、图表历史序列与回测输入，当开发者审核这些实现时，那么系统能够指出哪些部分已经符合新契约、哪些部分仍需要后续故事修正。

## 任务 / 子任务

- [x] 在规划文档中冻结 RPS 业务定义。 (AC: 1, 2)
  - [x] 在 `prd.md` 或相关规划文档中补充 RPS 计算公式、排名宇宙范围、价格口径与缺失数据处理规则。
  - [x] 明确 `翻红` 等图表观察性状态是否属于正式策略信号；如果不是，必须标记为解释性信息而非筛选依据。
  - [x] 明确 screen、chart、backtest 是否共享同一日频时间点与同一套规范化口径。
- [x] 审核并固化派生事实契约。 (AC: 2, 3)
  - [x] 盘点 `derived_indicator_daily`、`screen_runs`、`stock detail payload`、`backtest` 当前消费的 RPS 字段与语义。
  - [x] 形成一份明确的契约说明：哪些字段是权威判定值，哪些字段是历史序列，哪些字段只用于解释展示。
  - [x] 标注当前实现中仍依赖旧假设或待后续故事修正的点。
- [x] 为后续实现故事建立开发护栏。 (AC: 2, 3)
  - [x] 给 `3.5` 和 `5.4` 提供可直接引用的定义来源与验收边界。
  - [x] 明确不允许在前端生成未经后端持久化或可追溯重建的 RPS 权威曲线。
  - [x] 明确任何新的 RPS 图形状态如果参与筛选或解释，必须先进入文档和测试。
- [x] 验证文档一致性。 (AC: 1, 2, 3)
  - [x] 逐项核对 `prd.md`、`epics.md`、`sprint-change-proposal` 与现有实现故事，确保措辞一致。
  - [x] 记录需要由后续开发故事落地的代码与测试改动范围。

## 开发备注

- 这是一次 `correct-course` 之后新增的修正故事，目标不是直接改代码，而是先把 RPS 的业务语义锁死，防止技术实现继续建立在漂移定义上。 [Source: _bmad-output/planning-artifacts/sprint-change-proposal-2026-04-14-191615.md]
- Epic 2 已经实现了 RPS 50 / 120 / 250 的派生事实持久化，但当前 `2.2` 的原始故事只要求“算出来并存下来”，没有要求把公式、排名宇宙、不可计算区间与版本语义说清楚。 [Source: _bmad-output/implementation-artifacts/2-2-materialize-rps-and-52-week-high-derived-facts.md, _bmad-output/planning-artifacts/epics.md:321-339]
- Epic 3 已经实现真实历史 RPS 序列替换前端伪曲线，但这只能说明“图上不再画假线”，不能自动证明“图上画的是用户真正要看的业务曲线”。 [Source: _bmad-output/implementation-artifacts/3-4-replace-manual-stock-detail-svg-with-lightweight-charts.md]
- 架构文档已经要求 screening、charting、backtesting 使用同一套 stored facts，并要求对 incomplete-data 保持显式表达。本故事需要把这种架构原则进一步具体化为 RPS 语义契约。 [Source: _bmad-output/planning-artifacts/architecture.md:246-305,335-336]
- 研究文档仍保留关键开放问题：“What exact rule defines `翻红` for the RPS curves in your UI and screen logic?” 这说明 RPS 图表语义此前并未真正闭环。 [Source: _bmad-output/planning-artifacts/research/technical-stock-backtesting-jp-us-research-2026-04-13.md:1134-1137]

## 实施指导

- 本故事优先修改文档与定义边界，不应在没有冻结定义之前直接改 `factor_materialization.py`、`chart_data.py` 或 `backtesting.py` 的语义实现。
- 结论必须足够具体，能被后续开发故事直接引用；不要写成“待确认更多细节”的空泛占位。
- 如果某个问题当前没有结论，例如 `翻红` 是否属于正式信号，必须把它明确列为开放问题，并在 story 中限制后续实现不得擅自假定。
- 现有实现中与 RPS 强相关的关键文件包括：
  - `apps/api/src/stockanalyse_api/services/factor_materialization.py`
  - `apps/api/src/stockanalyse_api/services/chart_data.py`
  - `apps/api/src/stockanalyse_api/services/backtesting.py`
  - `apps/web/src/components/stocks/StockDetailCharts.tsx`
  - `apps/web/src/components/stocks/StockDetailView.tsx`
- 不要引入新的前端权威计算路径。前端只能消费后端提供的权威事实或被明确标记为解释性信息的数据。

## 架构符合性

- 与现有架构保持一致：后端继续作为 screening outputs、chart-ready datasets 与 backtest inputs 的事实源。
- 保持 SQLite MVP 路径兼容，不在本故事中引入超出定义冻结所需的 schema 变更。
- 所有与 RPS 定义相关的调整都必须服务于“同一 stored dataset、同一 semantic definition、同一 explainability chain”。

## 测试要求

- 本故事以文档与契约审查为主，但必须输出后续可执行的测试清单。
- 至少要明确后续开发需覆盖：
  - RPS 派生事实计算的定义一致性测试
  - chart detail 返回的 `indicator_history` 与 screen run 判定值一致性测试
  - backtest 与 screen 使用同一 RPS 语义的回归测试
  - 缺失或不可计算 RPS 历史的显式状态测试

## 上一故事情报

- `2.4` 已经把结果列表、最佳 RPS 和通过条件摘要展示出来，因此 `2.5` 不应重新设计结果页，而是为这些摘要建立更可靠的定义来源。 [Source: _bmad-output/implementation-artifacts/2-4-display-screen-result-list-with-qualification-summary.md]
- 最近提交里已经有一次“`claude en to cn`”相关整理，说明当前文档语言正在中文化，本故事应延续中文文档输出，不再新增英文 story 内容。 [Source: git log]

## 参考资料

- Epic 定义: [epics.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/epics.md:380)
- 变更提案: [sprint-change-proposal-2026-04-14-191615.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/sprint-change-proposal-2026-04-14-191615.md:1)
- 派生事实故事: [2-2-materialize-rps-and-52-week-high-derived-facts.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/implementation-artifacts/2-2-materialize-rps-and-52-week-high-derived-facts.md)
- 结果列表故事: [2-4-display-screen-result-list-with-qualification-summary.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/implementation-artifacts/2-4-display-screen-result-list-with-qualification-summary.md)
- 图表修正故事: [3-4-replace-manual-stock-detail-svg-with-lightweight-charts.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/implementation-artifacts/3-4-replace-manual-stock-detail-svg-with-lightweight-charts.md)

## 开放问题

- `翻红` 是否属于正式策略信号，还是仅属于图表观察性注释？
- RPS 排名宇宙是否固定为“当日可交易且数据完整的支持证券集合”，还是另有排除规则？
- RPS 计算应统一使用 `adj_close`，还是在某些场景允许退回 `close`？
- 是否需要为历史 screen run / backtest run 引入 `definition version` 以解释旧结果？

## 开发代理记录

### 使用的代理模型

GPT-5.4

### 调试日志参考

- 新增 `_bmad-output/planning-artifacts/rps-semantics-contract.md`，冻结 MVP 阶段 RPS 的公式、排名宇宙、价格口径、不可计算规则以及正式信号边界。
- 更新 `prd.md`，将 RPS 的权威定义与 explainability 边界绑定到统一契约。
- 更新 `architecture.md`，要求 derived facts、chart payload 与 backtest inputs 统一遵循同一份 RPS 语义契约。
- 更新 `epics.md` 中 2.5 的 AC，使其要求单一契约文档作为后续故事的直接引用来源。
- 通过 `git diff --check` 发现 `epics.md` 尾随空格后已清理。
- 使用 `rg` 核对 `rps-semantics-contract.md`、`prd.md`、`architecture.md`、`epics.md` 与本故事之间的关键引用和术语一致性。

### 完成说明

- 已冻结 MVP 阶段 RPS 语义，明确当前正式定义版本为 `rps-v1-2026-04-14`。
- 已写清当前实现实际采用的规则：优先 `adj_close`、缺失回退 `close`、按可计算证券集合做横截面收益百分位排名。
- 已明确 `翻红` 不属于当前正式筛选或回测信号，只能视为待定义的解释性概念。
- 已把权威判定值、权威历史序列与解释性图形状态的边界写入契约文档，供 `3.5` 与 `5.4` 直接引用。
- 已记录当前实现已符合项与待后续故事修正项，包括 `definition version` 尚未持久化、UI 语义分层仍待补强。
- 本故事没有新增代码逻辑，因此没有新增自动化测试；本次验证基于文档引用检查与 `git diff --check` 格式检查完成。

### 文件清单

- _bmad-output/implementation-artifacts/2-5-freeze-rps-business-definition-and-derived-fact-contract.md
- _bmad-output/implementation-artifacts/sprint-status.yaml
- _bmad-output/planning-artifacts/rps-semantics-contract.md
- _bmad-output/planning-artifacts/prd.md
- _bmad-output/planning-artifacts/architecture.md
- _bmad-output/planning-artifacts/epics.md

### 变更日志

- 2026-04-14: 创建中文 story，上下文已对齐 correct-course 提案、Epic 2 / 3 / 5 与现有 RPS 实现边界。
- 2026-04-14: 冻结 RPS 语义契约，更新 PRD / Architecture / Epics 引用，并将故事推进到 review。
