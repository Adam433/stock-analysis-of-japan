# 故事 1.6: Correct Universe Manifest Freshness Display and Common-Stock Count Semantics

状态: done

## 用户故事

作为用户，  
我希望数据健康页面能够正确展示普通股清单规模和清单更新时间，  
以便我能够信任该页面展示的 Universe 覆盖度与运营状态，而不是被异常数字或本地路径信息误导。

## 验收标准

1. 假设数据健康摘要被渲染，当产品显示 universe 相关信号时，那么它使用正确的 approved common-stock manifest 语义显示普通股清单数量，并把 manifest 更新时间作为主要新鲜度信号。
2. 假设 Universe manifest 来自本地文件，当主数据健康摘要展示时，那么 UI 不依赖原始本地文件路径作为主要用户可见的信任信号。
3. 假设 market data health API 返回 manifest 元数据，当首页和相关信任横幅展示时，那么普通股清单数量、更新时间和已存行情覆盖数量彼此语义清晰，不把它们混成同一个概念。
4. 假设 manifest 不存在或元数据缺失，当数据健康页面展示相关区域时，那么页面显示显式缺失状态，而不是显示误导性的路径或错误数量。

## 任务 / 子任务

- [ ] 修正 backend 的 universe manifest 快照语义。 (AC: 1, 4)
  - [ ] 审查 `apps/api/src/stockanalyse_api/services/health.py` 中 `UniverseManifestSnapshot` 的字段设计，区分“用户应见元数据”和“仅供内部实现使用的路径信息”。
  - [ ] 明确普通股数量的来源与统计口径，确保来自 approved TSE common-stock manifest，而不是页面拼装、副作用字段或错误子集。
  - [ ] 为 manifest 缺失、存在但可读取、时间戳存在等场景补充后端测试。
- [ ] 修正首页数据健康区的 universe 展示。 (AC: 1, 2, 3, 4)
  - [ ] 调整 `apps/web/src/app/page.tsx` 中覆盖度卡片与详情区的展示，使主界面只显示普通股清单更新时间，而不是本地文件路径。
  - [ ] 保留必要的覆盖度信息，但明确区分 `已存日频行情涵盖 X 只标的` 与 `普通股清单 Y 只 / 更新时间 Z` 的不同含义。
  - [ ] 在 manifest 缺失时给出明确缺失提示。
- [ ] 同步共享 trust 文案与前端使用点。 (AC: 2, 3)
  - [ ] 审查 `apps/web/src/components/shared/WorkflowTrustBanner.tsx` 是否也使用了旧 universe 文案或字段，并同步修正。
  - [ ] 确保数据健康页和共享 trust banner 对 universe 信息的表达一致。
- [ ] 通过测试验证语义修正。 (AC: 1, 3, 4)
  - [ ] 扩展 `apps/api/tests/test_market_data_health.py`，覆盖 manifest 元数据的正确返回与缺失场景。
  - [ ] 如前端有对应测试则更新；若无现成测试，至少完成针对受影响组件的 lint / build 验证。

## 开发备注

- 本故事直接承接 Epic 1 的数据健康与运营可信视图范围，属于对 `1.4` 和 `1.5` 的增量修正，而不是新开一套健康语义。 [Source: _bmad-output/planning-artifacts/epics.md]
- PRD 已更新为要求暴露 `universe manifest freshness`，并明确主 UI 不应把本地文件路径作为主要信任信号。 [Source: _bmad-output/planning-artifacts/prd.md]
- 架构补充说明要求 data health 分开报告 `stored market-data coverage`、`approved common-stock universe size`、`universe manifest last-updated timestamp`、`refresh execution state`。本故事只聚焦其中的 manifest 语义，不主动扩展到自动 refresh 调度。 [Source: _bmad-output/planning-artifacts/architecture.md]
- UX 补充说明要求数据健康页优先展示信任信号而非实现细节，并将 raw source path 降为非主视图信息。 [Source: _bmad-output/planning-artifacts/ux-followups-2026-04-15.md]
- 当前后端实现中 `UniverseManifestSnapshot` 暴露了 `source_path`，前端首页也直接渲染了 `Universe 清单来源：${universeManifest.source_path}`。这正是本故事要消除的主要误导点之一。 [Source: apps/api/src/stockanalyse_api/services/health.py, apps/web/src/app/page.tsx]
- 当前共享 trust banner 已经显示 `东京证券交易所普通股清单 {symbol_count} 只，更新时间 {updated_at}`，因此首页主视图与共享组件之间已经出现表达不一致。实现时应统一语义，不要让两个入口继续分叉。 [Source: apps/web/src/components/shared/WorkflowTrustBanner.tsx]
- 现有测试 `test_health_reports_universe_manifest_metadata` 已验证 `symbol_count`、`universe_filter` 和 `source_path`。如果移除或降级 `source_path` 的主接口意义，需要同步更新该测试及可能受影响的序列化断言。 [Source: apps/api/tests/test_market_data_health.py]

## Implementation Guidance

- 优先做“语义收敛”，不要先改文案再让 backend 字段继续含混。
- 如果需要保留 `source_path` 供调试使用，尽量将其变为非主视图字段，避免默认首页路径泄漏。
- 不要改变 `coverage_status`、`freshness_state`、`last_refresh` 的既有契约，除非本故事的实现确实要求并且相关使用点同时更新。
- SQLite、本地 manifest 文件、以及现有 JPX common-stock workflow 都保持不变；本故事只修正展示与健康语义。

## References

- Epic story definition: [epics.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/epics.md:313)
- PRD updates: [prd.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/prd.md:393)
- Architecture addendum: [architecture.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/architecture.md:882)
- UX supplement: [ux-followups-2026-04-15.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/ux-followups-2026-04-15.md:54)
- Existing health service: [health.py](/Users/adam/Documents/GitHub/stockAnalyse/apps/api/src/stockanalyse_api/services/health.py:1)
- Existing homepage health UI: [page.tsx](/Users/adam/Documents/GitHub/stockAnalyse/apps/web/src/app/page.tsx:163)
- Shared trust banner: [WorkflowTrustBanner.tsx](/Users/adam/Documents/GitHub/stockAnalyse/apps/web/src/components/shared/WorkflowTrustBanner.tsx:55)
- Existing backend tests: [test_market_data_health.py](/Users/adam/Documents/GitHub/stockAnalyse/apps/api/tests/test_market_data_health.py:238)
- Previous story context: [1-5-fix-homepage-refresh-messaging-and-support-full-universe-ingestion.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/implementation-artifacts/1-5-fix-homepage-refresh-messaging-and-support-full-universe-ingestion.md)

## Git Intelligence

- Recent commits indicate the repo has just been updated around K-line/story planning and DB work, so this story should avoid broad unrelated refactors while the worktree is still moving. [Source: `git log --oneline -5`]

## Open Questions

- 是否需要在 secondary detail view 保留 `source_path` 供调试，还是完全从 API 响应中移除？
- `普通股清单数量` 当前异常是否纯展示问题，还是 manifest 文件本身存在同步偏差？实施时应先验证数据来源再定 UI 文案。

## Dev Agent Record

### Agent Model Used

GPT-5

### Debug Log References

- CC proposal approved on 2026-04-15
- Story created from updated PRD / architecture / UX / epics set

### Completion Notes List

- Removed raw universe manifest path from the primary API/UI contract and kept the user-facing health summary focused on count and timestamp semantics.
- Unified homepage and shared trust banner wording so universe metadata is presented consistently.
- Added backend coverage for manifest-present and manifest-missing cases.

### File List

- _bmad-output/planning-artifacts/prd.md
- _bmad-output/planning-artifacts/architecture.md
- _bmad-output/planning-artifacts/ux-followups-2026-04-15.md
- _bmad-output/planning-artifacts/epics.md
- apps/api/src/stockanalyse_api/services/health.py
- apps/web/src/app/page.tsx
- apps/web/src/components/shared/WorkflowTrustBanner.tsx
- apps/api/tests/test_market_data_health.py
