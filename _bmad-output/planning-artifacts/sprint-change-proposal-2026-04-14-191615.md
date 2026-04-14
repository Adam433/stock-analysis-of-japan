# Sprint Change Proposal: RPS 曲线定义与实施纠偏

日期: 2026-04-14 19:16:15 JST
模式: Incremental
触发类型: Failed approach requiring different solution + Misunderstanding of original requirements
建议范围分类: Major

## 1. Issue Summary

### 1.1 触发问题

当前项目中，RPS 曲线被定义为 MVP 的核心能力，但实施过程中曾出现“以最新快照外推伪历史曲线”的错误实现路径，导致 RPS 图形展示一度不具备可验证性与可信度。

该问题并不只是一个前端渲染缺陷，而是暴露出更深层的问题:

- RPS 的业务定义尚未被完整锁定
- RPS 曲线的“展示口径”与“筛选口径”没有在规划层统一
- 需求文档把 RPS 当作已明确定义的能力，但研究文档仍保留关键开放问题
- 已完成的 stories 默认 RPS 50/120/250 和阈值高亮已可直接实施，但“什么是正确的 RPS 曲线”并未在规划中被充分约束

### 1.2 发现依据

- PRD 将 “RPS 50/120/250 calculation and visualization” 列为 MVP 核心能力。`_bmad-output/planning-artifacts/prd.md`
- Epic 2 / Epic 3 将 RPS 计算、RPS panel、阈值高亮、rule breakdown 全部视为已规划完成项。`_bmad-output/planning-artifacts/epics.md`
- Story 3.4 明确记录: 旧 RPS panel “visually misleading because it extrapolates a fake time series from the latest snapshot”。`_bmad-output/implementation-artifacts/3-4-replace-manual-stock-detail-svg-with-lightweight-charts.md`
- 前端当前文案也明确承认“已移除前端伪造衰减曲线”。`apps/web/src/components/stocks/StockDetailCharts.tsx`
- 研究文档仍存在开放问题: “What exact rule defines 翻红 for the RPS curves in your UI and screen logic?”，说明 RPS 曲线语义在规划阶段并未真正闭合。`_bmad-output/planning-artifacts/research/technical-stock-backtesting-jp-us-research-2026-04-13.md`

### 1.3 核心问题陈述

当前项目的 RPS 功能虽然已有计算、存储、筛选、详情展示和图表实现，但其“业务语义定义”并未在 PRD / Epic / Architecture 层完成闭环，导致已完成实现可能只是在技术上可运行，却未必符合用户真正要看的 RPS 曲线定义与判定规则。

如果不先纠偏，后续围绕 RPS 的筛选、解释、回测与 UI 信任链都会建立在不稳定前提之上。

## 2. Checklist 状态

### 2.1 Understand the Trigger and Context

- [x] 1.1 触发 story 已识别
  触发 story 主要是 Story 3.2 与后续修正 Story 3.4。3.4 直接暴露了旧 RPS 曲线为伪造历史轨迹的问题。
- [x] 1.2 核心问题已定义
  问题类型属于“错误方案暴露出原始需求理解不完整”。
- [x] 1.3 初步证据已收集
  证据来自 PRD、epics、研究文档、实现 story 与当前代码。

### 2.2 Epic Impact Assessment

- [x] 2.1 当前 epic 可否按原计划完成
  不能完全按“原理解”视为完成。Epic 2 与 Epic 3 需要被重新打开并补充语义定义与验收边界。
- [x] 2.2 所需 epic 级变更
  需要修改现有 epic 范围与验收标准，并新增围绕 RPS 语义定义和重算验证的 story。
- [x] 2.3 检查后续 epic 影响
  Epic 5 回测逻辑也会受影响，因为回测必须与同一套 RPS 语义保持一致。
- [x] 2.4 是否导致未来 epic 失效或需新增
  不需要新增独立大 Epic，但需要在现有 Epic 2 / 3 / 5 下新增修正 story，必要时加一个 cross-cutting correction story。
- [x] 2.5 是否应调整优先级
  是。所有新增功能应暂停让位于 RPS 语义纠偏与重验证。

### 2.3 Artifact Conflict and Impact Analysis

- [x] 3.1 PRD 存在冲突
- [x] 3.2 Architecture 存在冲突
- [!] 3.3 UI/UX 规格缺口明显
  当前没有独立 UX 文档，但 PRD 与实现页面都假定“RPS panel 的含义已清晰”。这需要补足。
- [x] 3.4 其他 artifacts 受影响
  实现 stories、测试、screen/backtest 语义、说明文档都需要同步修订。

### 2.4 Path Forward Evaluation

- [x] 4.1 Option 1: Direct Adjustment
  可行，但前提是先补齐 RPS 定义文档，再修改 stories 与实现。Effort: Medium-High。Risk: Medium。
- [x] 4.2 Option 2: Potential Rollback
  局部可行，不建议整体代码回滚。应回滚的是“已宣称完成”的计划状态，而不是盲目回滚代码。Effort: Medium。Risk: Medium。
- [x] 4.3 Option 3: PRD MVP Review
  MVP 不必放弃 RPS，但必须收缩为“仅支持已明确定义、可验证的 RPS 语义”。Effort: Medium。Risk: Low-Medium。
- [x] 4.4 推荐路径
  选择 Hybrid: 先修订 PRD / Epic / Architecture / Story 验收口径，再基于新定义做实现调整与验证，不做大范围代码硬回滚。

### 2.5 Sprint Change Proposal Components

- [x] 5.1 问题摘要已形成
- [x] 5.2 Epic 与 artifacts 影响已整理
- [x] 5.3 推荐路径已给出
- [x] 5.4 MVP 影响与高层行动计划已给出
- [x] 5.5 handoff 计划已给出

### 2.6 Final Review and Handoff

- [x] 6.1 检查项已覆盖
- [x] 6.2 Proposal 初稿已形成
- [!] 6.3 待用户批准
- [!] 6.4 待批准后更新 `sprint-status.yaml`

## 3. Impact Analysis

### 3.1 Epic Impact

受影响 epic:

- Epic 2: Strategy configuration, derived facts, screen runs
- Epic 3: Stock detail, chart review, explainability
- Epic 5: Backtesting reproducibility

影响判断:

- Epic 2 受影响，因为 FR15/16 默认将 RPS 50/120/250 视为已正确定义，但实际上只锁定了 lookback，未锁定完整业务语义。
- Epic 3 受影响，因为 chart-level verification 的前提是图上画出的 RPS 历史序列就是用户要验证的那条曲线。
- Epic 5 受影响，因为 backtest 若沿用当前 RPS 语义，结果可重复但不一定正确。

### 3.2 PRD 冲突

冲突点:

- PRD 多处把 “RPS calculation and visualization” 当作已定义能力，但未写出:
  - RPS 的精确定义
  - 曲线展示的数据来源与采样规则
  - “翻红”或其他关键图形状态的定义
  - screen、chart、backtest 是否使用同一时间点和同一口径
- PRD 的信任链要求“user can verify visually and numerically”，但此前实现曾使用伪历史曲线，这说明现有 PRD 约束不够强。

### 3.3 Architecture 冲突

冲突点:

- 架构文档强调 “stored facts” 和 “same dataset”，这是对的，但还不够。
- 现在缺的是:
  - RPS derived facts 的语义定义
  - 历史 RPS 曲线 API 的 contract
  - RPS 时间窗与 price 时间窗对齐原则
  - 数据缺口、不可计算区间、停牌与新股窗口的处理规则

### 3.4 UI / Explainability 冲突

冲突点:

- 当前 UI 可展示真实历史序列，但“展示真实历史序列”本身不等于“展示正确的业务曲线”。
- 阈值线、最佳 RPS、通过判定、历史三条曲线之间的关系需要更明确的 UX 说明。
- 用户看到曲线时究竟要验证:
  - 当日筛选是否成立
  - 历史期间是否持续满足
  - 曲线是否“翻红”
  这些目前没有被明确拆分。

### 3.5 Secondary Artifact Impact

受影响的二级 artifacts:

- 实现 stories: `2-2`, `2-3`, `3-1`, `3-2`, `3-3`, `3-4`, `5-1`, `5-2`, `5-3`
- API contract 与测试
- 前端文案与 explainability copy
- 研究结论摘要
- sprint status 中对 Epic 2 / 3 / 5 “done” 的状态可信度

## 4. Recommended Approach

### 4.1 推荐方案

采用 Hybrid 方案:

1. 先冻结新增功能开发
2. 修订 PRD 中与 RPS 相关的功能定义与验收口径
3. 修订 Epic 2 / 3 / 5 中受影响 stories 的 Acceptance Criteria
4. 新增一组 correction stories，专门完成 RPS 语义重定义、历史曲线 contract 重校准、筛选与回测一致性验证
5. 基于新 story 再评估哪些已有代码需要修改、废弃或保留

### 4.2 不建议的路径

- 不建议直接继续开发其他功能
- 不建议只修 UI 文案或图表样式
- 不建议仅凭“现在已经不是 fake curve 了”就宣布问题解决
- 不建议大面积 `git revert`，因为问题核心是规划与语义，不是单次提交错误

### 4.3 理由

- 当前问题触及产品可信度核心
- RPS 是 MVP 主干，不是边缘能力
- 已有实现有可复用部分，故不需要全盘回滚
- 但若不先重写计划文档，后续每个实现修正都会继续漂移

## 5. Detailed Change Proposals

### 5.1 PRD 修改提案

#### PRD Proposal A: 将“RPS calculation and visualization”改为“定义明确且可验证的 RPS 语义”

位置:

- `_bmad-output/planning-artifacts/prd.md` MVP / Functional Requirements / User Journeys 中所有 RPS 相关段落

OLD:

- RPS calculation and visualization for 50-day, 120-day, and 250-day periods
- threshold-based highlighting for RPS conditions

NEW:

- 采用明确业务定义的 RPS 50/120/250 计算规则，并在 screen、chart、backtest 中共享同一语义
- RPS 图表仅展示由后端持久化或可追溯重建的真实历史指标序列
- 明确区分:
  - 当日筛选判定值
  - 历史指标曲线
  - 图表观察性状态定义，例如“翻红”或其他视觉事件
- 任何 RPS 图形状态若参与筛选或解释，必须在需求中被明确定义并具备测试覆盖

Rationale:

把“可画出来”提升为“业务可验证”，堵住伪曲线与错误语义再次进入系统的口子。

#### PRD Proposal B: 新增 RPS 开放问题收敛清单

新增 section:

- RPS exact formula
- percentile/ranking universe definition
- adjusted vs raw price policy
- missing history handling
- IPO / sparse-history handling
- `翻红` 是否只是视觉解释，还是正式筛选信号

Rationale:

这些问题目前已经实际影响实现，不能继续留在研究文档里悬空。

### 5.2 Epic / Story 修改提案

#### Story 2.2 修改

Story: `2-2-materialize-rps-and-52-week-high-derived-facts`
Section: Acceptance Criteria

OLD:

- system computes and stores 50-day, 120-day, and 250-day RPS-related values

NEW:

- system computes and stores 50-day, 120-day, and 250-day RPS values according to the approved business definition
- system records enough lineage metadata to explain how each RPS value was derived
- system explicitly marks dates or securities where RPS is not computable under the approved definition

Rationale:

当前 AC 只要求“算出来”，没有要求“按哪种定义算”“无法算时怎么表达”。

#### Story 2.3 修改

Story: `2-3-execute-screen-runs-and-persist-results`
Section: Acceptance Criteria

OLD:

- each qualified stock is linked to the stored values that caused it to pass

NEW:

- each qualified stock is linked to the exact stored RPS definition version, indicator values, and threshold logic that caused it to pass

Rationale:

否则未来改了 RPS 口径后无法解释旧 run。

#### Story 3.1 修改

Story: `3-1-serve-stock-detail-and-chart-data-from-stored-facts`

NEW 增补:

- backend returns both current screening values and historical RPS series using the same approved semantic definition
- payload makes unavailable or partial RPS history explicit instead of silently fabricating continuity

#### Story 3.2 修改

Story: `3-2-build-stock-detail-page-with-candlestick-and-rps-panels`

OLD:

- page displays an RPS panel below the main price chart

NEW:

- page displays RPS history only from approved and traceable backend indicator history
- the page clearly distinguishes screening threshold status, historical series, and any observational chart annotation

Rationale:

避免 UI 再次把“解释性图形”误当成“正式指标曲线”。

#### Story 3.3 修改

Story: `3-3-show-rule-breakdown-and-exact-qualifying-values`

NEW 增补:

- rule breakdown explicitly identifies whether a displayed visual state is explanatory-only or part of official screen logic

#### Story 3.4 状态修正建议

Story: `3-4-replace-manual-stock-detail-svg-with-lightweight-charts`

建议:

- 保留已完成的技术替换成果
- 但把“已解决信任问题”的结论降级为“移除了已知伪曲线实现风险，仍待 RPS 业务定义最终确认”

Rationale:

3.4 修复了一个错误实现，但没有单独解决规划层的定义缺口。

### 5.3 新增 Correction Stories 提案

建议新增以下 stories:

- Story CC-1: 定义并冻结 RPS 业务语义
  - 明确公式、排名宇宙、价格口径、缺失处理、版本策略
- Story CC-2: 重新校准 RPS derived facts contract
  - 如需 schema 或 API contract 调整，在此完成
- Story CC-3: 统一 screen / chart / backtest 的 RPS 解释链
  - 通过测试与可追溯输出验证一致性
- Story CC-4: 重写 RPS explainability UX
  - 明确“筛选判定值”“历史曲线”“观察性注释”三者关系

## 6. MVP Impact

### 6.1 MVP 是否仍成立

成立，但条件是:

- 保留 RPS 作为 MVP 核心能力
- 缩小承诺范围，只交付“已明确定义且可验证”的 RPS
- 暂不把任何未定义清楚的曲线事件或图形语义写成正式筛选能力

### 6.2 时间线影响

- 预计会打断当前开发顺序
- 需要先完成 planning artifact 更新，再重开开发
- 若 RPS 定义牵涉数据库或回测 contract，Epic 5 的已完成结论也要重新验证

### 6.3 风险

- 风险最高项不是编码，而是“业务定义仍然模糊”
- 如果继续跳过定义澄清，系统会得到更多 technically-correct but semantically-wrong 的产物

## 7. Handoff Plan

### 7.1 推荐 handoff

Major scope，建议路由给:

- Product Manager / Product Owner
  - 修订 PRD 中的 RPS 范围、定义和 MVP 边界
- Architect
  - 修订 derived facts、history payload、versioning 和一致性策略
- Developer
  - 在新 stories 批准后，修改 API、materialization、detail payload、UI explainability 与测试

### 7.2 成功标准

- PRD 中 RPS 定义不再含糊
- epics/stories 的 AC 可以直接判定对错
- screen、chart、backtest 对同一股票同一日期给出一致解释
- UI 不再显示任何未经业务定义支持的伪曲线或伪状态

## 8. Immediate Next Actions

1. 批准本次 change proposal
2. 更新 PRD 的 RPS 定义段落
3. 更新 `epics.md` 中 2.2 / 2.3 / 3.1 / 3.2 / 3.3 的 AC
4. 新建 correction stories
5. 将 `sprint-status.yaml` 中受影响 epic 从“done”调整为更真实的状态
6. 再进入 `bmad-create-story` 与 `bmad-dev-story`

## 9. Approval Status

当前状态: Approved on 2026-04-14

待确认问题:

- RPS 的最终业务定义是什么
- `翻红` 是否属于正式策略信号
- 是否需要为旧 screen/backtest runs 引入 definition version
- 是否需要把 Epic 5 一并重新打开
