# Sprint Change Proposal: Integrate Chart, Data-Health, and Screening Follow-Ups from TodoList

## 1. Issue Summary

当前 sprint 进行过程中，用户在真实使用中集中发现了 9 个增量问题，分布在 4 个已有能力面：

- 数据健康页的数据口径与展示方式
- Stock detail 图表可用性与可读性
- 筛选参数能力边界
- 观察列表到股票详情页的工作流连通性

这些问题不是单一 bug。它们混合了三类变化：

1. 已实现功能的展示缺陷或错误口径
2. 既有工作流中缺失但用户预期明确的能力补全
3. 需要影响既有 API、后台任务或数据契约的中等范围增强

触发这些变化的现有 story / epic 主要是：

- Epic 1
  - `1-4` Expose data freshness and refresh status
  - `1-5` Fix homepage refresh messaging and support full-universe ingestion
- Epic 2
  - `2-1` Create strategy configuration workflow
  - `2-2` Materialize RPS and 52-week-high derived facts
  - `2-3` Execute screen runs and persist results
  - `2-7` Select screening trade date from available derived-fact dates
- Epic 3
  - `3-1` Serve stock detail and chart data from stored facts
  - `3-2` Build stock detail page with candlestick and RPS panels
  - `3-4` Replace manual stock detail SVG with lightweight charts
  - `3-5` Clarify RPS chart semantics and explainability boundaries
- Epic 4
  - `4-3` View and review the watchlist

本次变更的直接证据来自 `TodoList.md` 中的 9 条用户观察，包括：

- K 线历史长度明显不足
- RPS 标签遮挡最近曲线
- 日期格式不符合中文使用预期
- 数据健康页顶部导航结构异常
- Universe 来源显示异常，且用户只关心更新时间
- 刷新执行状态没有自动推进
- RPS 筛选仍固定在 `50/120/250`，无法配置窗口与满足数量
- 观察列表无法直接跳转股票详情
- 数据健康页普通股清单数量显示异常，如 `普通股清单：71 只`

问题本质判断：

- `2 / 3 / 4 / 8` 是现有需求下的 UX 与工作流补全
- `5 / 9` 是数据健康语义或统计逻辑错误
- `1 / 6 / 7` 已经构成产品能力和技术边界变化，需要回写 planning artifacts

## 2. Impact Analysis

### Epic Impact

- **Epic 1 受影响最大**
  - 数据健康页 Universe 来源与普通股清单数量口径需要修正
  - 刷新执行状态需要从“展示最近一次状态”提升为“启动即触发、运行中按日自动维护”
- **Epic 2 需要扩展**
  - 当前筛选能力默认只支持固定 `RPS50 / 120 / 250`
  - 需要增加“自定义 RPS 窗口列表”和“至少满足 N 个 RPS 条件”的参数化规则
- **Epic 3 需要扩展与修正**
  - 图表历史数据长度不足
  - 图表日期格式与标签布局需要修正
  - 数据健康页导航异常也说明共享页面壳层存在一致性问题
- **Epic 4 需要补充一个轻量 story**
  - 观察列表项应能直接进入股票详情页
- **Epic 5 / Epic 6**
  - 不需要新增 epic
  - 但 Epic 5 的回测参数体系未来应与 Epic 2 的可配置 RPS 窗口保持一致，存在后续联动风险

### Story Impact

- 现有 stories 不需要回滚
- 需要新增后续 stories，建议如下：
  - `1-6` 修正数据健康页 Universe 元数据与普通股统计口径
  - `1-7` 启动时触发并按日维护刷新执行状态
  - `2-8` 支持参数化 RPS 窗口与最少满足数量
  - `3-6` 扩展 stock detail 图表历史数据与可读性修复
  - `4-4` 从观察列表直接访问股票详情
- 现有 stories 中需要澄清但不回滚的项：
  - `1-4` 和 `1-5` 的“refresh status”范围过窄，未覆盖自动维护
  - `3-4` 和 `3-5` 已完成图表替换与语义澄清，但未覆盖标签避让、日期格式、本地化显示和历史长度策略
  - `2-1` / `2-3` 建立了配置与执行流程，但默认假设固定 RPS 集合

### Artifact Conflicts

#### PRD

当前 PRD 存在以下缺口：

- 仅明确 MVP 使用 `50/120/250` RPS 语义，但没有说明后续是否允许参数化窗口
- 已要求数据健康可见，但没有要求系统在 backend 启动或每日自动推进 refresh 状态
- 已要求 candlestick 与 RPS review，但没有明确历史加载范围、标签避让、本地化日期格式
- 已要求 watchlist review，但没有明确从 watchlist drill into stock detail 的路径
- 已要求 Universe / freshness trust，但没有要求只显示 Universe 文件更新时间，也没有明确普通股清单统计口径

#### Architecture

当前 architecture 需要补充的点：

- stock detail chart data API 的历史窗口策略
  - 默认返回更长历史
  - 或支持按交互分页 / 增量加载
- operations / refresh 子系统需要支持：
  - backend 启动时自动创建或推进 refresh run
  - 服务持续运行时按日触发维护
- screening configuration 需要支持：
  - 可变长度 RPS window 集合
  - “至少满足 N 个 RPS 条件”的求值语义
- data health 需要明确：
  - Universe manifest 更新时间来源
  - approved common-stock universe 统计口径

#### UI/UX

当前没有独立 UX 文档，但已有页面行为显然受影响：

- chart 默认显示范围
- chart 日期格式
- RPS 标签布局与遮挡策略
- 顶部导航一致性
- watchlist 到详情页入口
- 数据健康页文案与密度

这意味着虽然没有现成 UX artifact，需要补一个轻量 UX 补充说明。

### Technical Impact

- Backend API 可能需要扩展 chart data 返回范围或分页参数
- Backend screening configuration / evaluation 需要支持动态 RPS 窗口输入
- Derived facts 可能需要支持更多窗口的即时或持久化计算策略
- Refresh scheduler / startup hooks 需要加入新行为
- Data health service 需要修正 universe 计数和文件 metadata 读取方式
- Frontend shared layout / navigation 需要统一处理
- Watchlist list item 需要新增跳转 affordance

## 3. Recommended Approach

推荐路径：**Hybrid，以 Direct Adjustment 为主，辅以 PRD / UX / Architecture 定点更新**

### 选项评估

#### Option 1: Direct Adjustment

- **Viable**
- Effort: Medium
- Risk: Medium

原因：

- 不需要推翻现有 epic 结构
- 绝大多数问题可以通过新增 stories 或修正文档边界解决
- 现有实现可继续复用，不需要大规模重构

#### Option 2: Potential Rollback

- **Not viable**
- Effort: High
- Risk: High

原因：

- 当前问题不是方向选错，而是实现粒度、边界定义和部分统计逻辑不完整
- 回滚 `Epic 1-4 / 1-5 / 3-4 / 3-5 / 2-1 / 2-3` 只会损失已完成工作，不会减少真实需求复杂度

#### Option 3: PRD MVP Review

- **Partially viable but not recommended as primary path**
- Effort: Medium
- Risk: Medium

原因：

- MVP 本身仍然成立，不需要降 scope
- 但 PRD 需要小幅增补，尤其是对参数化 RPS、watchlist drill-down、数据健康自动维护、chart 可用性边界的描述

### Selected Path

**Hybrid: Direct Adjustment + Targeted Planning Artifact Updates**

具体做法：

1. 用增量方式补充 PRD、轻量 UX 说明、Architecture
2. 在既有 epics 下新增 stories，而不是创建新 epic
3. 重新做 sprint planning，把数据正确性问题排在最前
4. 从第一个新增 story 开始进入 `create-story`

### Rationale

- 能保留当前实现成果和 sprint momentum
- 能把真正有架构影响的项显式文档化，避免之后重复返工
- 风险主要集中在参数化 RPS 与 refresh 自动化，这两项需要先写清边界再开发

### MVP Impact

- MVP **不被否定**
- 但 MVP 的“可用且可信”标准需要补全为：
  - 数据健康展示口径正确
  - refresh 状态能自动推进
  - chart review 不因数据长度与标签遮挡降低可用性
  - watchlist 是完整研究流，不是孤立列表
  - screening parameters 不再被固定 RPS 窗口硬编码死

## 4. Detailed Change Proposals

### PRD

#### 4.1 Screening Parameterization

OLD:

- `RPS calculation and visualization for 50-day, 120-day, and 250-day periods using one approved business definition across screening, charting, and backtesting`
- FR15: `The product can calculate 50-day, 120-day, and 250-day RPS-related values for supported securities.`
- FR16: `The product can determine whether at least one supported RPS line satisfies the strategy threshold condition.`

NEW:

- `RPS calculation and visualization shall preserve the approved business definition while supporting a configurable set of user-selected RPS lookback windows.`
- FR15 revised: `The product can calculate approved RPS-related values for supported securities for the configured lookback windows required by screening, chart review, and backtesting.`
- FR16 revised: `The product can determine whether at least a user-configured minimum number of selected RPS lines satisfy the strategy threshold condition.`
- New FR56: `The user can configure which RPS lookback windows participate in the active screening rule.`
- New FR57: `The user can configure how many selected RPS lines must satisfy the threshold condition for a security to qualify.`

Rationale:

- 当前固定 `50/120/250` 已不足以覆盖用户的实际策略迭代需求

#### 4.2 Data Health and Refresh Automation

OLD:

- FR12: `The product can expose the freshness state of the stored market data.`
- FR48: `The user can see whether market data is current enough for routine post-close use.`
- FR49: `The user can see when a data update has failed, is incomplete, or may affect output trustworthiness.`

NEW:

- FR12 revised: `The product can expose the freshness state, universe manifest freshness, and refresh execution state of the stored market data.`
- FR48 revised: `The user can see whether market data and the approved universe manifest are current enough for routine post-close use.`
- FR49 revised: `The user can see when a data update has failed, is incomplete, stale, or has not been automatically advanced as expected.`
- New FR58: `The backend can trigger or maintain refresh execution state automatically at startup and on the expected daily cadence.`
- New FR59: `The product can display the last-updated timestamp of the approved universe manifest without exposing unnecessary local file path details in the primary UI.`

Rationale:

- 当前数据健康页存在口径错误与自动推进缺口，已经影响“运营可信视图”

#### 4.3 Chart Review and Watchlist Workflow

OLD:

- FR28: `The user can view a candlestick chart for a supported security.`
- FR29: `The user can view RPS information in a panel below the main price chart.`
- FR36: `The user can view the securities currently stored in the watchlist.`

NEW:

- FR28 revised: `The user can view a candlestick chart for a supported security with sufficient historical context for routine chart review.`
- FR29 revised: `The user can view RPS information in a panel below the main price chart without important recent data being obscured by fixed labels.`
- New FR60: `The product can present chart dates in a localized, date-only format appropriate for the primary user workflow.`
- New FR61: `The user can navigate directly from a watchlist entry to the corresponding stock detail workflow.`

Rationale:

- 这些能力直接影响 chart review 与 watchlist continuity，不再只是视觉偏好

### Epics / Stories

#### 4.4 Epic 1 Additions

OLD:

- Epic 1 stories end at `1-5`

NEW:

- Add `Story 1.6: Correct Universe Manifest Freshness Display and Common-Stock Count Semantics`
- Add `Story 1.7: Maintain Refresh Execution State Automatically on Startup and Daily Cadence`

Rationale:

- `5 / 9 / 6` 都属于数据 backbone 与 operations trust 范围，应继续放在 Epic 1

#### 4.5 Epic 2 Addition

OLD:

- Epic 2 includes `2-7 Select Screening Trade Date from Available Derived-Fact Dates`

NEW:

- Add `Story 2.8: Parameterize RPS Windows and Minimum Satisfied-Line Count`

Rationale:

- 这比简单调参数更深，已经改变筛选配置和判定模型

#### 4.6 Epic 3 Addition

OLD:

- Epic 3 stories end at `3-5`

NEW:

- Add `Story 3.6: Expand Stock Detail Chart History and Improve Chart Readability`

Story scope:

- 扩大 K 线默认历史范围，或引入常规交互式增量加载
- 修正 RPS 标签遮挡问题
- 统一 chart 日期格式为中文 date-only
- 修正数据健康页顶部导航一致性问题，如果该问题来自共享页面壳层

Rationale:

- `1 / 2 / 3 / 4` 共享同一条 frontend/chart shell 上下文，合并为一个可交付单元更合理

#### 4.7 Epic 4 Addition

OLD:

- Epic 4 stories end at `4-3`

NEW:

- Add `Story 4.4: Navigate from Watchlist Entries to Stock Detail`

Rationale:

- 这是 watchlist continuity 的自然补全，不应塞到 Epic 3

### Architecture

#### 4.8 Chart Data Serving

OLD:

- stock detail and chart serving are defined at a high level

NEW:

- stock detail chart data serving must return enough historical depth for routine pattern review, either through a larger default payload or a well-defined incremental loading contract
- frontend must remain thin; authoritative data windows and any pagination semantics must be backend-defined

Rationale:

- 避免前端自行拼接或猜测历史窗口

#### 4.9 Screening Evaluation Model

OLD:

- MVP strategy assumes approved `50/120/250` RPS set

NEW:

- the approved RPS business definition remains fixed, but the screening rule input may specify a configurable set of lookback windows and a minimum satisfied-line count
- architecture must define whether non-default windows are materialized ahead of time, computed on demand from stored prices, or supported through a bounded approved window set

Rationale:

- 需要先限定技术边界，避免需求进入无限制指标工厂

#### 4.10 Operations / Refresh Automation

OLD:

- local job execution path supports refresh workflows

NEW:

- backend runtime may initialize or maintain refresh execution state automatically on startup
- if the backend remains running, a daily automation path must update refresh status on the expected schedule
- architecture must clarify how this interacts with SQLite locking, existing maintenance jobs, and manual refresh commands

Rationale:

- 自动刷新不是单纯 UI 更改，涉及运行时职责边界

#### 4.11 Data Health Semantics

OLD:

- data freshness visibility is required

NEW:

- data health responses must separately report:
  - stored market-data coverage
  - approved common-stock universe size
  - universe manifest update timestamp
  - refresh execution state
- UI should not expose raw local filesystem paths as the primary trust signal

Rationale:

- 当前页面把路径和错误口径混在一起，信号质量太差

### UX

#### 4.12 UX Supplement Needed

OLD:

- no UX design artifact exists

NEW:

- create a lightweight UX supplement covering:
  - stock detail chart default range and load-more behavior
  - RPS line label placement rules
  - chart date localization
  - top navigation consistency
  - watchlist detail-entry affordance
  - data health page information hierarchy

Rationale:

- 本轮修改有多个 UI 决策点，不补 UX 说明会导致后续 story 粒度过粗

## 5. Implementation Handoff

### Scope Classification

- **Moderate**

原因：

- 不需要重做产品方向
- 但需要 backlog 重组、多个 planning artifacts 增量更新，以及跨 Epic 新增 stories

### Recommended Handoff Recipients

- **Product Owner / Planning**
  - 用 `EP` 回写 PRD
  - 用 `CU` 补轻量 UX 说明
  - 用 `CA` 回写 architecture 边界
  - 用 `CE` 生成新增 stories
- **Developer**
  - 在 `SP` 之后按优先级进入 story 实施

### Recommended Sequencing

1. `EP` 更新 PRD
2. `CU` 补 UX 说明
3. `CA` 更新 architecture
4. `CE` 生成新增 stories
5. `SP` 重新排 sprint 顺序
6. `CS` 创建第一个 next story

### Recommended Story Priority

1. `1-6` 修正 Universe / common-stock count / 更新时间口径
2. `1-7` 自动维护 refresh execution state
3. `3-6` 图表历史与可读性修复
4. `4-4` watchlist 到 detail 跳转
5. `2-8` 参数化 RPS 窗口与最少满足数量

排序理由：

- 先恢复数据健康可信度
- 再修正 chart review 和研究工作流断点
- 最后处理影响面最大的筛选能力扩展

### Success Criteria

- 数据健康页不再显示错误的普通股数量口径
- 数据健康页主视图只展示 Universe 更新时间，不展示本地文件路径
- refresh execution state 在 backend 启动及持续运行场景下能自动推进
- stock detail 图表提供足够历史上下文，且 RPS 标签不遮挡关键近期数据
- chart 日期使用中文 date-only 格式
- watchlist entry 可直接进入股票详情页
- screening rule 支持配置 RPS 窗口集合和最少满足数量

