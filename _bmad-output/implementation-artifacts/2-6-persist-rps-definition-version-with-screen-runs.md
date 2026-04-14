# 故事 2.6: 持久化 Screen Run 使用的 RPS 定义版本

状态: done

## 用户故事

作为运营者，  
我希望每次 screen run 都记录它使用的是哪一个已批准的 RPS 定义版本，  
以便 RPS 契约后续演进后，历史结果仍然可解释、可追溯。

## 验收标准

1. 假设存在一个已批准的 RPS 语义契约版本，当用户执行一次 screen run 时，那么持久化的 run 记录会保存该次执行使用的 RPS 定义版本或等价契约标识。
2. 假设后续需要调查某次历史筛选结果，当运营者查看 run 上下文与合格结果时，那么他可以直接看到控制该次结果的 RPS 定义版本，而不需要反查代码提交历史。

## 任务 / 子任务

- [x] 为 `screen_runs` 增加 RPS 定义版本存储。 (AC: 1, 2)
  - [x] 为 `ScreenRun` 模型增加 `rps_definition_version` 或等价字段。
  - [x] 新增对应 migration，并保持现有 SQLite MVP 路径可升级。
  - [x] 为已存在 run 的兼容策略做出最小且清晰的处理，不要引入含糊默认值。
- [x] 在 screen 执行链路中写入契约版本。 (AC: 1)
  - [x] 让 `execute_screen_run` 在创建 `ScreenRun` 时写入当前批准的 RPS 定义版本。
  - [x] 版本来源必须与 `_bmad-output/planning-artifacts/rps-semantics-contract.md` 中冻结的契约一致，而不是散落硬编码在多个地方。
  - [x] 如果当前实现暂时无法自动读取规划文档，也必须在代码中集中定义一个单一来源常量，避免多处字符串漂移。
- [x] 在读取与序列化路径中暴露该版本。 (AC: 2)
  - [x] 更新 `ScreenRunSummary` 与相关序列化输出，让 `GET /screen/runs/{id}` 与 `GET /screen/runs/latest` 可返回该版本。
  - [x] 确保未来 stock detail 或 explainability 调查链路可以复用该字段，而不是重新推断。
- [x] 验证 screen run 追溯链。 (AC: 1, 2)
  - [x] 扩展 `tests/test_screening.py`，覆盖 run 创建后版本已保存、读取后版本可见。
  - [x] 验证缺失派生事实的失败路径不会伪造版本上下文。
  - [x] 运行 backend 单元测试、编译检查和 migration 升级校验。

## 开发备注

- 这是对 `2.3` 的补强故事。此前 screen run 已经能保存参数集、阈值和单股判定值，但还不能显式说明“当时遵循的是哪一版 RPS 语义契约”。 [Source: _bmad-output/implementation-artifacts/2-3-execute-screen-runs-and-persist-results.md]
- `2.5` 已经冻结当前 MVP 的契约版本为 `rps-v1-2026-04-14`，本故事要把这个版本真正落进运行记录。 [Source: _bmad-output/planning-artifacts/rps-semantics-contract.md]
- 当前 `ScreenRun` 只有 `strategy_configuration_id`、`trade_date`、`executed_at`、`total_candidates`、`qualified_count`、`status`，还没有契约版本字段。 [Source: apps/api/src/stockanalyse_api/domain/screens/models.py]
- 当前 `execute_screen_run` 只返回参数集版本和阈值，没有返回 RPS 语义版本。 [Source: apps/api/src/stockanalyse_api/services/screening.py]
- 这个故事只负责 screen run。backtest run 的对应能力由 `5.5` 负责，不要在这里顺手混做。

## 实施指导

- 版本来源要单一。优先方案是集中在 backend 可复用的位置定义，例如 `services` 或单独的 semantic-constants 模块；不要在 route、service、test 里各写一份字符串。
- 不要尝试在运行时直接解析 markdown 规划文档来驱动业务逻辑，除非仓库里已经有稳定模式。对当前项目，更稳妥的是在代码里维护一个与契约文档同步的单一常量，并在 story 完成说明里写明同步点。
- 序列化输出里，`parameter_set.version` 与新的 `rps_definition_version` 语义不同，不能混用。
- 如果 migration 需要为旧记录提供值，优先考虑显式回填当前冻结版本；如果这会产生历史误导，则应该保持为空并在读取时清楚暴露“未知/未记录”。

## 架构符合性

- 保持“screening、charting、backtesting 对同一 stored dataset 和同一 semantic definition 保持一致”的架构原则。 [Source: _bmad-output/planning-artifacts/architecture.md]
- 该字段属于运行追溯元数据，放在 `screen_runs` 层比散落在 `screen_run_results` 更符合现有模型边界。
- 本故事不应改变筛选结果算法，只增加运行上下文的可追溯性。

## 测试要求

- 至少补以下测试：
  - screen run 创建后 `rps_definition_version` 已写入数据库
  - `get_screen_run` / `get_latest_screen_run` 返回该字段
  - 失败路径不伪造成功的版本上下文
- 继续运行当前 screening 回归测试，确保新增字段不破坏既有结果摘要和 qualified result 序列化。

## 上一故事情报

- `2.5` 已经把契约版本、正式信号边界和开放问题定义清楚，因此 `2.6` 不需要再次讨论 RPS 公式本身，而是要把该结论落入运行记录。 [Source: _bmad-output/implementation-artifacts/2-5-freeze-rps-business-definition-and-derived-fact-contract.md]
- `2.3` 建立了 screen run / screen run result 的持久化骨架，所以最自然的实现位置是扩展现有模型和 service，而不是引入新的追溯表。 [Source: _bmad-output/implementation-artifacts/2-3-execute-screen-runs-and-persist-results.md]

## 参考资料

- Epic 定义: [epics.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/epics.md:399)
- 契约文档: [rps-semantics-contract.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/rps-semantics-contract.md)
- 上一故事: [2-5-freeze-rps-business-definition-and-derived-fact-contract.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/implementation-artifacts/2-5-freeze-rps-business-definition-and-derived-fact-contract.md)
- Screen run 实现: [screening.py](/Users/adam/Documents/GitHub/stockAnalyse/apps/api/src/stockanalyse_api/services/screening.py:77)
- Screen run 模型: [models.py](/Users/adam/Documents/GitHub/stockAnalyse/apps/api/src/stockanalyse_api/domain/screens/models.py:22)
- 现有测试: [test_screening.py](/Users/adam/Documents/GitHub/stockAnalyse/apps/api/tests/test_screening.py:1)

## 开放问题

- 旧有 screen runs 是否应该回填为 `rps-v1-2026-04-14`，还是保留为空以表示“历史未记录”？
- stock detail payload 是否应在后续故事中直接暴露 `screen_run.rps_definition_version`？

## 开发代理记录

### 使用的代理模型

GPT-5.4

### 调试日志参考

- 新增 `apps/api/src/stockanalyse_api/services/rps_semantics.py`，集中定义 `APPROVED_RPS_DEFINITION_VERSION = "rps-v1-2026-04-14"`，作为 screen run 的单一语义版本来源。
- 为 `ScreenRun` 模型增加 `rps_definition_version` 字段，并新增 migration `20260415_0013_add_screen_run_rps_definition_version.py`。
- 更新 `apps/api/src/stockanalyse_api/services/screening.py`，在 `execute_screen_run` 创建 run 时写入版本，并在 `ScreenRunSummary` / `get_screen_run` / `get_latest_screen_run` 返回该字段。
- 扩展 `apps/api/tests/test_screening.py`，覆盖 run 持久化、读取链路和缺失派生事实时不产生 run 记录的失败路径。
- 验证通过：
  - `PYTHONPATH=src python3 -m unittest tests.test_screening`
  - `PYTHONPATH=src python3 -m compileall src`
  - `PYTHONPATH=src python3 -m alembic -c alembic.ini upgrade head`

### 完成说明

- `screen_runs` 现在可以持久化 `rps_definition_version`，screen 结果不再只能依赖参数集版本和代码历史做解释。
- Screen 执行链路在 run 创建时固定当前批准的 RPS 契约版本，后续读取接口可直接返回该值。
- 版本来源已集中到单一常量模块，避免 route、service、tests 各自维护字符串。
- 缺失派生事实的失败路径不会伪造空的 screen run 记录，因此不会产生误导性的版本追溯上下文。

### 文件清单

- _bmad-output/implementation-artifacts/2-6-persist-rps-definition-version-with-screen-runs.md
- _bmad-output/implementation-artifacts/sprint-status.yaml
- apps/api/src/stockanalyse_api/domain/screens/models.py
- apps/api/src/stockanalyse_api/services/rps_semantics.py
- apps/api/src/stockanalyse_api/services/screening.py
- apps/api/migrations/versions/20260415_0013_add_screen_run_rps_definition_version.py
- apps/api/tests/test_screening.py

### 变更日志

- 2026-04-14: 创建中文 story，覆盖 `screen_runs` 的 definition-version 持久化、读取链路和测试要求。
- 2026-04-14: 为 screen run 落地 `rps_definition_version` 持久化与读取链路，并补充后端回归测试。
