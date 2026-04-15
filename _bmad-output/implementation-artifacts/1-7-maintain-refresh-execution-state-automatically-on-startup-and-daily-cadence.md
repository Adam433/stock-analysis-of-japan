# 故事 1.7: Maintain Refresh Execution State Automatically on Startup and Daily Cadence

状态: done

## 用户故事

作为用户，  
我希望 refresh execution state 在后端启动时自动推进，并在服务持续运行时按日维护，  
以便数据健康页面反映真实的运营状态，而不是依赖手工触发后才有状态变化。

## 验收标准

1. 假设 backend 服务启动，当运行时初始化时，那么系统会按批准的自动化规则创建或推进 refresh execution state。
2. 假设 backend 在预期刷新周期内持续运行，当每日自动触发时间到达时，那么 refresh execution state 会自动推进，并且结果可从数据健康工作流看到。
3. 假设一次自动 refresh 转换失败或被跳过，当用户查看产品状态时，那么该状态会被显式暴露，而不会表现成静默成功。

## 任务 / 子任务

- [ ] 为运行时自动 refresh 增加受控配置入口。 (AC: 1, 2, 3)
  - [ ] 设计 startup auto-refresh 是否启用、使用哪个 provider、是否跑全量 universe、daily cadence 间隔等配置项。
  - [ ] 保证默认行为不会在测试环境中无控制地发起真实网络刷新。
- [ ] 在 API 运行时增加自动推进机制。 (AC: 1, 2, 3)
  - [ ] 在 `apps/api/src/stockanalyse_api/main.py` 引入受控 startup hook / lifespan 逻辑。
  - [ ] 复用现有 ingestion provider registry 与 `execute_market_data_refresh`，而不是发明第二套 refresh 语义。
  - [ ] 为“服务持续运行时按日维护”实现轻量、可停止的后台循环或等价机制。
- [ ] 将自动执行结果保持在现有 health 语义中可见。 (AC: 1, 2, 3)
  - [ ] 自动成功、部分成功、失败都要落在 `market_data_refresh_runs` 里，并能通过 `/health/market-data` 读出。
  - [ ] 不要引入只存在于内存、health API 无法观察到的伪状态。
- [ ] 为运行时自动化补测试。 (AC: 1, 2, 3)
  - [ ] 增加运行时自动 refresh 协调逻辑的单元测试。
  - [ ] 覆盖 startup 触发、失败可见、以及“未到 cadence 不重复跑”的情况。

## 开发备注

- 本故事聚焦“自动推进 refresh execution state”，不是一次性重做完整夜间维护脚本。应尽量复用现有 `execute_market_data_refresh` 记录的 `MarketDataRefreshRun` 语义。 [Source: apps/api/src/stockanalyse_api/services/ingestion/refresh_service.py]
- 当前 API 启动路径 `create_app()` 只注册路由和 CORS，没有 startup hook、lifespan 或后台调度器。 [Source: apps/api/src/stockanalyse_api/main.py]
- 现有维护脚本 `scripts/maintenance/sync_universe_and_refresh.sh` 已体现推荐 provider 选择顺序：默认 `yahoo_finance_chart`，再 materialize derived facts。这是运行时自动化的重要参考，但不应未经约束地在每次测试时直接执行整套脚本。 [Source: scripts/maintenance/sync_universe_and_refresh.sh]
- 架构补充说明已经限定：backend runtime 可以在 startup 时推进 refresh execution status；持续运行时应支持 daily automation；实现必须兼容 SQLite locking、现有手动命令与明确的 refresh jobs。 [Source: _bmad-output/planning-artifacts/architecture.md]
- PRD 只要求自动 trigger / maintain refresh execution state，并未强制在 startup 时完成完整数据物化或同步 universe manifest。因此实现可先限定为 market-data refresh run 的自动推进。 [Source: _bmad-output/planning-artifacts/prd.md]
- 如果使用 `yahoo_finance_chart` provider，测试必须避免真实网络依赖；需要通过可注入 provider / coordinator 结构实现可测性。 [Source: apps/api/src/stockanalyse_api/services/ingestion/providers/registry.py, apps/api/src/stockanalyse_api/services/ingestion/providers/yahoo_finance_chart_provider.py]

## Implementation Guidance

- 优先把自动化协调逻辑封装成独立服务，再由 FastAPI startup/lifespan 调用。
- 默认测试路径必须可禁用自动 refresh，避免让 `TestClient` 或导入 app 时触发真实副作用。
- 如果需要 cadence 控制，优先使用简单明确的“上次自动尝试时间 + 当前时间”规则，不要一开始引入重量级调度依赖。
- 自动 refresh 失败也应写入 `market_data_refresh_runs`，这样 health API 才能显式看到失败。

## References

- Epic story definition: [epics.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/epics.md:332)
- PRD updates: [prd.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/prd.md:454)
- Architecture addendum: [architecture.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/architecture.md:910)
- Existing app startup: [main.py](/Users/adam/Documents/GitHub/stockAnalyse/apps/api/src/stockanalyse_api/main.py:1)
- Existing refresh execution service: [refresh_service.py](/Users/adam/Documents/GitHub/stockAnalyse/apps/api/src/stockanalyse_api/services/ingestion/refresh_service.py:1)
- Existing refresh job entrypoint: [refresh_market_data.py](/Users/adam/Documents/GitHub/stockAnalyse/apps/api/src/stockanalyse_api/jobs/refresh_market_data.py:1)
- Existing maintenance script: [sync_universe_and_refresh.sh](/Users/adam/Documents/GitHub/stockAnalyse/scripts/maintenance/sync_universe_and_refresh.sh:1)
- Refresh run persistence model: [models.py](/Users/adam/Documents/GitHub/stockAnalyse/apps/api/src/stockanalyse_api/domain/operations/models.py:1)

## Open Questions

- startup 自动化是否只跑 market-data refresh，还是也应顺带触发 universe sync / derived fact materialization？
- daily cadence 是基于固定本地时刻，还是“距离上次自动尝试超过 N 小时”的滚动规则？

## Dev Agent Record

### Agent Model Used

GPT-5

### Debug Log References

- Story created after 1-6 implementation and review cycle completed.

### Completion Notes List

- Added a controlled auto-refresh runtime that starts with FastAPI lifespan and evaluates refresh cadence in the background.
- Reused persisted `market_data_refresh_runs` so automatic success and failure states remain visible through the existing health API.
- Added runtime tests covering startup lifecycle, due/not-due cadence, provider execution failure, and provider-build failure visibility.

### File List

- _bmad-output/planning-artifacts/prd.md
- _bmad-output/planning-artifacts/architecture.md
- _bmad-output/planning-artifacts/epics.md
- apps/api/src/stockanalyse_api/main.py
- apps/api/src/stockanalyse_api/services/ingestion/refresh_service.py
- apps/api/src/stockanalyse_api/jobs/refresh_market_data.py
- apps/api/src/stockanalyse_api/domain/operations/models.py
- scripts/maintenance/sync_universe_and_refresh.sh
