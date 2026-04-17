# Sprint Change Proposal: Page Review Follow-Ups from Local UI Verification

## 1. Issue Summary

本次变更触发来自本地页面验收记录 `test result.md`，共确认 4 类问题：

1. 首页顶部导航样式与 `/screen` 等 workflow 页面不一致。
2. `/screen` 首屏即出现 `无法加载策略配置（500）`、`无法加载最近一次筛选结果（500）`，且筛选按钮不可用。
3. 策略配置中的“最少满足条数”不再符合当前需求，用户要求取消该参数。
4. 个股详情页返回 `详情数据暂不可用。无法加载个股详情（500）。`

补充技术证据：

- 后端运行日志已确认 `/screen/configuration` 和 `/stocks/:id/detail` 的 500 根因是当前运行 SQLite 库缺少
  `strategy_configurations.selected_rps_windows` 列。
- 因此第 2、4 项首先是运行库 schema / migration 状态问题，其次才是页面级回归验证问题。

后续在继续本地使用时，又确认了一组更底层的工作流问题：

5. 用户希望“启动筛选”直接使用当前表单参数试跑，而不是先保存参数集才能运行。
6. 当前版本号（如 `v5`）在筛选工作流里缺少可读性，用户难以判断某次筛选究竟使用了哪套参数。
7. 用户更关心“加入观察时自动带上当次筛选参数”，而不是维护大量历史参数集版本。
8. 用户希望筛选结果区不仅显示股票列表，还能直接看到 K 线图、估值指标和净利润柱状图；若结果过多，应支持滚动增量加载。
9. 用户强调后续方案应尽量避免“造轮子”，优先复用成熟组件、现有框架能力和已存在的数据契约。
10. 用户明确指出当前回测页的“启动回测 / 执行最新任务”逻辑不可接受，按钮语义必须调整为用户可理解的流程。
11. 用户明确指出当前“回测”定义不符合预期：希望从某次筛选结果出发，计算组合后续收益、胜率、最大回撤和收益曲线，而不是只统计历史上有多少次命中条件；第一版还需要纳入止损规则。

## 2. Impact Analysis

### Epic / Story Impact

- **Epic 1 / 运行环境与可信视图**
  - 运行数据库 schema 未跟上已实现代码，导致 workflow 页面无法正常载入。
- **Epic 2 / Story 2.8**
  - 当前 story 定义为“批准窗口 + 最少满足条数”，与最新用户期望冲突。
  - 需要将 story 语义收窄为“只选择批准窗口，不再配置满足条数”。
- **Epic 2 / Story 2.1 / Story 2.3**
  - 当前实现把“保存参数集”和“启动筛选”绑定得过紧，导致用户试跑临时参数时必须先创建正式版本。
  - 需要重定义筛选工作流中“当前编辑参数”“已保存参数集”“screen run 参数快照”的关系。
- **Epic 4 / Story 4.1 / Story 4.2**
  - 当前 watchlist 只保存用户手工上下文，没有自动沉淀当次筛选的参数背景。
  - 需要评估在加入观察时自动写入筛选参数、交易日、run id 的方式。
- **Epic 5 / Story 5.1 / Story 5.2**
  - 回测仍然适合强调基于已保存参数集的可复现性，但需要与筛选工作流的“临时试跑”语义拆开。
  - 当前 UI 将“创建 run”与“执行 run”直接暴露成两个不直观按钮，需重构回测交互语义。
  - 当前服务层实现的是“历史条件命中统计”，不是用户理解的收益回测；Epic 5 的目标定义需要重写。
- **Epic 3 / Story 3.1 / 3.2 / 3.6**
  - 当前图表与财务分析主要集中在 stock detail 页面，不满足“筛选结果区直接比较候选股”的需求。
  - 需要评估筛选结果卡片的图表/财务数据契约，以及是否通过分页或滚动加载降低首屏压力。
  - 实现策略上应优先复用现有 charting、列表加载与服务端数据聚合能力，避免为结果区额外发明一套定制基础设施。
- **Epic 3**
  - 个股详情页依赖 `StrategyConfiguration` 读取，同样受到 schema 不匹配影响。
- **Epic 6**
  - 首页与 workflow 页导航风格不统一，属于跨页面体验一致性问题。

### Artifact Conflicts

- `epics.md` 中 Story 2.8 需要更新。
- 历史实现故事 `2-8-parameterize-rps-windows-and-minimum-satisfied-line-count.md` 已完成，不建议直接覆盖历史；后续应通过修正实现或新增 follow-up story 落地。
- `TodoList.md` 需要纳入本次页面验收发现的问题。
- `epics.md`、`prd.md` 中关于 FR5 / FR7 / FR27 / FR34-FR40 / FR44 / FR46 的参数语义，需要做一次统一梳理，避免筛选与回测共享同一套过强的“先保存再运行”假设。
- `epics.md` 中 Epic 3 当前偏向“进入个股详情后再分析”，需要补一条“筛选结果区内联分析卡片”方向的 follow-up story。

## 3. Recommended Approach

建议按以下顺序处理：

1. **先修运行库 schema / migration 问题**
   - 这是 `/screen` 与个股详情 500 的直接根因。
   - 未修复前，其它页面行为验证都不可靠。
2. **再调整 Story 2.8 对应需求与实现**
   - 删除“最少满足条数”参数，统一前后端规则、展示文案与结果追溯字段。
3. **最后做首页导航统一**
   - 这是纯 UI 收口项，不阻塞主链路恢复。
4. **单独重做筛选参数工作流**
   - 将“试跑参数”和“正式保存参数集”拆开。
   - 在不破坏回测可复现性的前提下，弱化筛选页里的版本号中心地位。
   - 将当次筛选上下文向 watchlist 自动传递。
5. **补充筛选结果区分析能力**
   - 将筛选结果从纯列表升级为分析卡片流。
   - 评估 K 线、财务指标、净利润图的后端读取契约和前端滚动增量加载方案。
   - 方案优先复用成熟库与现有页面能力，而不是新增自定义图表协议或手写滚动框架。
6. **重构回测启动流程**
   - 将当前“先创建任务、再执行任务”的内部实现与用户交互分离。
   - 优先收敛为单次点击即可启动并执行的回测流程；若保留两步，则必须改成明确的任务语义。
7. **重定义回测目标**
   - 从“历史条件回放”改为“基于筛选结果组合的收益回测”。
   - 明确买入时点、持有周期、卖出规则、是否等权、是否允许调仓、基准比较、收益曲线与胜率统计口径。
   - 第一版收益回测需把止损作为明确规则，而不是后续可选项。

风险评估：

- schema 修复属于 **P0**，不修复则主要研究 workflow 无法使用。
- Story 2.8 变更属于 **中等影响**：会影响策略配置、筛选逻辑、回测逻辑、结果回显与测试。
- 首页导航统一属于 **低风险** UI 调整。
- 新发现的筛选参数工作流问题属于 **中到高影响**：会波及 Epic 2、Epic 4、Epic 5 的 story 边界与 API 契约。
- 新增的筛选结果分析卡片需求属于 **中等影响**：会波及 Epic 3 的数据契约、前端结果区结构，以及可能的按滚动分批加载机制。
- 回测按钮语义重构属于 **中等影响**：会波及 Epic 5 的启动/执行 story 拆分、前端交互和 API 命名语义。
- 回测目标重定义属于 **高影响**：会直接改变 Epic 5 的核心故事、后端计算模型、结果指标、页面结构与验收标准。

## 4. Detailed Change Proposals

### 4.1 TodoList

新增待办：

- `23. 运行库 schema 落后导致 /screen 与个股详情 500`
- `24. 筛选页与个股详情页需在修复 schema 后回归验证`
- `25. 首页导航样式与 workflow 页面不一致`
- `26. RPS 规则移除“最少满足条数”参数`

### 4.2 Story 2.8

**Artifact:** `_bmad-output/planning-artifacts/epics.md`

**OLD**

- Story title: `Parameterize RPS Windows and Minimum Satisfied-Line Count`
- AC required:
  - selectable approved windows
  - minimum satisfied-line count
  - invalid combinations where minimum count exceeds selected windows
  - backend evaluates selected windows + configured minimum count

**NEW**

- Story title: `Parameterize Approved RPS Windows`
- AC updated to:
  - selectable approved windows
  - at least one window must be selected
  - no separate minimum satisfied-line count parameter
  - backend evaluates the selected approved windows as the authoritative RPS condition set

**Rationale**

用户已明确否定“最少满足条数”这条策略输入，继续保留该参数会造成需求、实现和页面认知长期分叉。

### 4.3 Implementation Follow-Up

需要新增或执行的实现动作：

- 对实际运行 SQLite 库执行缺失 migration，并验证 schema 已包含：
  - `selected_rps_windows`
  - `min_rps_lines_required`（若后续需求确认彻底删除，则需要评估是否保留兼容列或继续迁移移除）
- 修正前后端策略配置与评估逻辑，使“勾选窗口即纳入条件”成为唯一交互模型。
- 更新首页顶部导航，使其与 workflow 页面共用相同导航风格。

### 4.4 Workflow Redesign Follow-Up

需要新增或修订的 story 方向：

- **Story 2.1 / 2.3**
  - `启动筛选` 应允许使用当前表单参数直接运行
  - `保存参数集` 才创建新的正式参数版本
  - `screen run` 需要保留自身的参数快照，而不是完全依赖 active configuration
- **Story 4.1 / 4.2**
  - watchlist entry 在加入时自动附带筛选上下文
  - 至少包括 `screen_run_id`、筛选交易日、关键筛选参数
- **Story 5.1 / 5.2**
  - 回测继续保留“已保存参数集 + 参数快照”的可复现模型
  - 与筛选工作流的临时试跑语义明确分离
  - 回测 UI 不应继续直接暴露“启动后还要再执行”的内部流程
  - 优先改成单步启动执行，或将按钮重命名为明确的任务语义（如“创建回测任务” / “执行该任务”）
  - 回测计算目标应从“命中统计”切换为“收益表现分析”，至少覆盖组合收益、胜率、最大回撤、收益曲线
  - 第一版需定义止损触发条件与执行价格口径

### 4.5 Result-Panel Analysis Follow-Up

需要新增或修订的 story 方向：

- **Story 3.1 / 3.2 / 3.6**
  - 筛选结果区应支持在候选卡片内直接展示价格图表与财务概览，而不必先跳转到 stock detail
  - 后端需要提供适合结果列表批量读取的轻量分析数据契约
  - 若结果数量较多，前端需要支持滚动触发的增量加载，而不是一次性渲染全部图表与财务面板
- **可能新增 Epic 3 follow-up story**
  - `Inline Screening Result Analysis Cards`
  - `Incremental Result-Panel Data Loading`

约束原则：

- 优先复用现有 `lightweight-charts` 图表能力、现有 stock detail 数据契约、以及成熟的滚动加载模式
- 避免为了筛选结果区单独设计一套新的图表引擎、前端可视化 DSL、或不必要的后端聚合框架

## 5. Implementation Handoff

### Scope Classification

**Moderate**

原因：

- 需要一次运行环境修复（migration / 启动流程）
- 需要一次产品规则修正（Story 2.8）
- 需要一次前端 UI 一致性收口
- 需要一次筛选参数工作流重构与 story 边界重整
- 需要一次筛选结果区分析能力扩展

### Recommended Handoff

- **Developer**
  - 修复运行库 schema 与启动流程
  - 调整策略配置、筛选/回测规则和相关页面
  - 统一首页导航
- **Product / Story Owner**
  - 确认 Story 2.8 新规则为最终产品方向
  - 确认筛选工作流是否正式改为“先试跑、后保存”
  - 确认 watchlist 是否自动沉淀筛选上下文
  - 确认筛选结果区是否以内联分析卡片取代当前纯列表，以及图表/财务数据的最小展示范围
- **Architecture / Developer**
  - 在方案评估时优先选择成熟依赖与现有契约复用，避免为新结果区需求引入自造基础设施

### Success Criteria

- `/screen` 首屏不再出现 500，筛选可正常启动。
- 个股详情页可正常打开。
- 策略配置页不再出现“最少满足条数”输入。
- 首页导航与 workflow 页面风格一致。
- `启动筛选` 与 `保存参数集` 的用户语义清晰分离。
- 用户可以明确看出某次筛选和某条 watchlist entry 使用了哪套参数上下文。
- 筛选结果区可以直接支持候选股的价格与财务快速比对，并在结果较多时保持可用性能。
