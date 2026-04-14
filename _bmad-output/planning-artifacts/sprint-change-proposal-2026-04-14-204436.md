# Sprint Change Proposal: Add Manual Screening Trade-Date Selection as Future Work

## 1. Issue Summary

当前 `screen` 工作流始终使用 `derived_indicator_daily` 中的最新交易日执行筛选。  
这在“日常跑最新筛选”场景下是合理的，但会带来一个明确缺口：

- 用户无法主动选择某个历史交易日重跑 screening
- 当前前端把“运行日期”展示为 run context，而不是可编辑输入
- 用户容易误以为该日期可以配置，实际上系统只是自动取最新派生事实日期

本问题是在实测中发现的：当 `derived_indicator_daily` 最晚只到某个旧日期时，screening 会固定跑在该日期，而且 UI 没有提供手动切换能力。

## 2. Impact Analysis

- Epic Impact:
  - 主要影响 `Epic 2`
- Story Impact:
  - 现有 `2.3` 仍成立，但其运行日期语义需要明确为“默认取最新派生事实日期”
  - 需要新增 future story 覆盖“手动选择 screening trade date”
- Artifact Conflicts:
  - `PRD` 目前只为 backtest 定义了历史日期选择能力，没有为 screening 定义同类需求
  - `epics.md` 中 Epic 2 尚无对应 story
  - `architecture.md` 需要补充 screening execution 的日期选择边界
- Technical Impact:
  - backend 需要让 screen run 支持显式 `trade_date`
  - frontend 需要在 screen workflow 增加“可选历史交易日选择器”
  - 需要明确只允许选择已有 `derived_indicator_daily` 的日期，而不是任意自然日

## 3. Recommended Approach

推荐路径：**Direct Adjustment, planned as future backlog**

- 不回滚现有 screen execution
- 保留当前“默认最新派生事实日期”的 MVP 行为
- 新增一个后续 story，为 screening 增加历史交易日选择能力
- 明确该能力必须继续复用同一套 persisted derived facts、run context 和 explainability chain

原因：

- 当前问题不是算法错误，而是产品能力边界未文档化
- 需求已经明确，适合直接进入 backlog，而不是继续靠隐含行为维持
- 该能力与 backtest 的日期选择心智一致，用户预期强

## 4. Detailed Change Proposals

### PRD

OLD:

- FR41: The user can launch a historical backtest for the MVP strategy.
- FR42: The user can select the historical date range used for a backtest.

NEW:

- 保留 FR41-FR54 不变
- 新增 FR55: The user can select an available historical trade date for a screening run when reviewing past market states.

Rationale:

- backtest 已支持历史日期范围；screening 缺同类能力，导致用户只能被动使用最新派生事实日期

### Epics

OLD:

- Epic 2 以 `2.6` 结束

NEW:

- 新增 `Story 2.7: Select Screening Trade Date from Available Derived-Fact Dates`

Rationale:

- 让该需求有明确交付单元，而不是散落在实现备注里

### Architecture

OLD:

- screening configuration and execution

NEW:

- screening configuration and execution
- screening execution may default to the latest derived-fact trade date, but future historical replay must select only from persisted derived-fact dates

Rationale:

- 明确“不是任意日期选择器”，而是“已物化派生事实日期选择器”

## 5. Implementation Handoff

- Scope: Moderate
- Handoff recipients:
  - Product / Planning: 保留 FR55 与 Story 2.7 作为后续 backlog
  - Developer: 后续实现时扩展 `/screen/runs` 输入、screen UI 日期选择器和相关测试
- Success criteria:
  - 用户可选历史交易日执行 screening
  - 所选日期必须存在于 `derived_indicator_daily`
  - screen result、stock detail、chart explainability 继续绑定同一日事实

