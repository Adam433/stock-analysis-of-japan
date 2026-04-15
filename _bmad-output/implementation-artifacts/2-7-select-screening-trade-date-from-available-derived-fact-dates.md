# 故事 2.7: Select Screening Trade Date from Available Derived-Fact Dates

状态: done

## 用户故事

作为用户，  
我希望能够从已有的 derived-fact 交易日中选择一次 screening 的运行日期，  
以便我可以回放某个历史市场状态，而不是永远只能跑最新一天。

## 验收标准

1. 假设存在多个已持久化的 derived fact 交易日，当用户打开 screening 工作流时，那么产品会展示一个明确的交易日选择器，其候选值来自可用的 derived-fact 日期。
2. 假设用户选择了一个历史交易日并启动筛选，当运行完成时，那么系统只针对该交易日的 stored derived facts 执行筛选，并在持久化 run context 中记录所选日期。
3. 假设用户没有手动选择交易日，当启动筛选时，那么产品仍默认使用最新可用的 derived-fact 交易日。
4. 假设用户请求了一个不存在于 derived-fact 集合中的日期，当后端验证请求时，那么接口返回显式错误，而不是默默回退到最新日期或任意自然日。

## 任务 / 子任务

- [ ] 扩展 screening service 支持可选 trade date。 (AC: 2, 3, 4)
  - [ ] 为 screening service 增加“可用 trade date 列表”查询。
  - [ ] 让 `execute_screen_run` 接受可选 trade date，并只允许使用 persisted derived-fact dates。
  - [ ] 保持未传日期时默认取最新 derived-fact 日期。
- [ ] 扩展 screening API。 (AC: 1, 2, 4)
  - [ ] 为 `/screen/runs` 增加可选请求体字段 `trade_date`。
  - [ ] 增加一个返回可用 screening trade dates 的接口，供前端选择器使用。
- [ ] 在策略配置页面增加历史 trade date 选择器。 (AC: 1, 2, 3)
  - [ ] 只显示来自 backend 的可用日期，不提供任意自然日自由输入。
  - [ ] 让运行反馈文案清楚说明本次筛选使用的是哪一天。
- [ ] 补测试与验证。 (AC: 1, 2, 3, 4)
  - [ ] 增加后端测试覆盖默认最新日期、显式历史日期、非法日期报错、可用日期列表。
  - [ ] 运行受影响前端的 lint 验证。

## 开发备注

- Story 2.7 是对既有 screen workflow 的增量扩展，不改变“screening 只能基于 persisted derived facts 执行”的核心边界。 [Source: _bmad-output/planning-artifacts/epics.md]
- 当前 `execute_screen_run(session)` 永远调用 `_latest_trade_date(session)`，因此后端还没有显式历史日期输入能力。 [Source: apps/api/src/stockanalyse_api/services/screening.py]
- 当前前端 `StrategyConfigPanel` 启动筛选时直接 `POST /screen/runs`，没有请求体，也没有 trade date 选择器。 [Source: apps/web/src/components/screen/StrategyConfigPanel.tsx]
- 先前的 CC proposal 已经明确：screening 的历史日期选择只能从 persisted derived-fact dates 中选择，不是任意自然日。 [Source: _bmad-output/planning-artifacts/sprint-change-proposal-2026-04-15-cc-todolist-followups.md]

## References

- Epic story definition: [epics.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/epics.md:418)
- Screening route: [screening.py](/Users/adam/Documents/GitHub/stockAnalyse/apps/api/src/stockanalyse_api/api/routes/screening.py:1)
- Screening service: [screening.py](/Users/adam/Documents/GitHub/stockAnalyse/apps/api/src/stockanalyse_api/services/screening.py:1)
- Strategy config panel: [StrategyConfigPanel.tsx](/Users/adam/Documents/GitHub/stockAnalyse/apps/web/src/components/screen/StrategyConfigPanel.tsx:1)
- Screen page: [page.tsx](/Users/adam/Documents/GitHub/stockAnalyse/apps/web/src/app/screen/page.tsx:1)

## Completion Notes

- Added a backend-supported list of available screening trade dates sourced only from persisted derived facts.
- Extended screen runs to accept an optional explicit trade date while preserving the latest-date default path.
- Added a frontend trade-date selector that still allows fallback-to-latest behavior when the trade-date list endpoint is unavailable.
