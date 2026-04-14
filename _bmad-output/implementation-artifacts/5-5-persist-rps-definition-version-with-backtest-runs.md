# 故事 5.5: 持久化 Backtest Run 使用的 RPS 定义版本

状态: done

## 用户故事

作为运营者，  
我希望每次 backtest run 都记录它使用的是哪一个已批准的 RPS 定义版本，  
以便契约更新之后，历史模拟结果仍然保持可复现、可比较。

## 验收标准

1. 假设存在一个已批准的 RPS 语义契约版本，当 backtest run 被创建并执行时，那么持久化的 backtest 记录会保存该次运行使用的 RPS 定义版本或等价契约标识。
2. 假设两个 backtest runs 跨时间被对比，当结果差异由契约版本变化引起时，那么运营者可以直接从 run 上下文看出 definition-version 差异，而不需要反查仓库历史。

## 任务 / 子任务

- [x] 为 `backtest_runs` 增加 RPS 定义版本存储。 (AC: 1, 2)
  - [x] 为 `BacktestRun` 模型增加 `rps_definition_version` 或等价字段。
  - [x] 新增对应 migration，并保持当前 SQLite MVP 开发路径可升级。
  - [x] 为历史 backtest 记录制定清晰兼容策略，不要引入模糊默认值。
- [x] 在 backtest 执行链路中写入契约版本。 (AC: 1)
  - [x] 让 `launch_backtest_run` 在创建 run 时写入当前批准的 RPS 定义版本。
  - [x] 版本来源必须与 `_bmad-output/planning-artifacts/rps-semantics-contract.md` 保持一致，不能在多处散落硬编码。
  - [x] 如果 `2.6` 已经引入集中常量或共享语义模块，本故事应复用同一来源，而不是再造一套。
- [x] 在读取与比较链路中暴露该版本。 (AC: 2)
  - [x] 更新 `BacktestRunSummary` 与相关序列化输出，让 `GET /backtests/runs`、`GET /backtests/runs/latest`、`GET /backtests/runs/{id}` 返回该字段。
  - [x] 确保 `/backtests` 页面后续可以直接利用该字段进行结果差异解释。
- [x] 验证回测追溯链。 (AC: 1, 2)
  - [x] 扩展 `tests/test_backtesting.py`，覆盖 run 创建后版本已保存、读取后版本可见。
  - [x] 验证无可用派生事实的失败路径不会伪造成功的版本上下文。
  - [x] 运行 backend 单元测试、编译检查和 migration 升级校验。

## 开发备注

- 这是 `5.4` 的运行上下文落地故事。`5.4` 关注“回测是否与批准的 RPS 语义保持一致”，`5.5` 负责把这种一致性写进可查询的持久化记录。 [Source: _bmad-output/planning-artifacts/epics.md]
- 当前 `BacktestRun` 只保存日期范围、状态、统计摘要和错误信息，没有 `rps_definition_version` 字段。 [Source: apps/api/src/stockanalyse_api/domain/backtests/models.py]
- 当前 `launch_backtest_run` 与 `_serialize` 只返回参数集版本与阈值，不返回 RPS 契约版本。 [Source: apps/api/src/stockanalyse_api/services/backtesting.py]
- `2.5` 已经冻结当前 MVP 契约版本为 `rps-v1-2026-04-14`。本故事必须使用同一版本来源，不允许再引入第二套命名。 [Source: _bmad-output/planning-artifacts/rps-semantics-contract.md]
- 如果 `2.6` 已经先完成，`5.5` 应尽量复用相同的 semantic-version 常量或 helper，保证 screen 和 backtest 真正共享同一来源。 [Source: _bmad-output/implementation-artifacts/2-6-persist-rps-definition-version-with-screen-runs.md]

## 实施指导

- 不要在 `execute_backtest_run` 里临时推断版本；版本应该在 run 创建时就固定下来，确保后续执行、重试和读取都指向同一上下文。
- `parameter_set.version` 与 `rps_definition_version` 是两个不同概念：前者是策略配置版本，后者是 RPS 业务语义版本，不能混淆。
- 不要为了这个故事改动回测算法本身；这里只增加运行追溯元数据与读取能力。
- 如果 migration 对旧 run 需要回填，处理原则应和 `2.6` 保持一致，避免 screen/backtest 在历史记录策略上出现分叉。

## 架构符合性

- 与架构文档保持一致：backtest 继续基于 stored inputs 和 persisted runs 工作，只扩展运行元数据。 [Source: _bmad-output/planning-artifacts/architecture.md]
- 该字段属于 run-level traceability metadata，放在 `backtest_runs` 模型上最符合现有数据边界。
- 本故事不应改变 `result_checksum`、合格统计或回测判定逻辑。

## 测试要求

- 至少补以下测试：
  - backtest run 创建后 `rps_definition_version` 已写入数据库
  - `get_backtest_run` / `get_latest_backtest_run` / `list_backtest_runs` 返回该字段
  - 失败路径不伪造成功的版本上下文
- 继续运行现有 backtesting 回归测试，确保新增字段不破坏摘要序列化与结果复现性测试。

## 上一故事情报

- `5.4` 已经把“回测必须与批准的 RPS 定义一致”提升成需求，因此 `5.5` 不需要再讨论公式，只需把“用了哪版定义”写入持久化上下文。 [Source: _bmad-output/planning-artifacts/epics.md:637]
- 当前 backtest 流程已经具备 run 持久化、结果摘要和 checksum，比起新建旁路追溯表，更自然的实现是扩展现有 `BacktestRun` 模型与 summary。 [Source: _bmad-output/implementation-artifacts/5-1-launch-and-persist-backtest-runs.md; _bmad-output/implementation-artifacts/5-2-execute-reproducible-backtests-from-stored-inputs.md]

## 参考资料

- Epic 定义: [epics.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/epics.md:655)
- 契约文档: [rps-semantics-contract.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/rps-semantics-contract.md)
- 对应 screen story: [2-6-persist-rps-definition-version-with-screen-runs.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/implementation-artifacts/2-6-persist-rps-definition-version-with-screen-runs.md)
- Backtest service: [backtesting.py](/Users/adam/Documents/GitHub/stockAnalyse/apps/api/src/stockanalyse_api/services/backtesting.py:34)
- Backtest model: [models.py](/Users/adam/Documents/GitHub/stockAnalyse/apps/api/src/stockanalyse_api/domain/backtests/models.py:13)
- 现有测试: [test_backtesting.py](/Users/adam/Documents/GitHub/stockAnalyse/apps/api/tests/test_backtesting.py:1)

## 开放问题

- 历史 backtest runs 是否应该统一回填 `rps-v1-2026-04-14`，还是保留为空表示“历史未记录”？
- `/backtests` 前端页面是否应在后续故事中直接显示 `rps_definition_version` 供用户比对？

## 开发代理记录

### 使用的代理模型

GPT-5.4

### 调试日志参考

- 复用 `apps/api/src/stockanalyse_api/services/rps_semantics.py` 中的 `APPROVED_RPS_DEFINITION_VERSION`，让 backtest 与 screen 使用相同的单一语义版本来源。
- 为 `BacktestRun` 模型增加 `rps_definition_version` 字段，并新增 migration `20260415_0014_add_backtest_run_rps_definition_version.py`。
- 更新 `apps/api/src/stockanalyse_api/services/backtesting.py`，在 `launch_backtest_run` 创建 run 时写入版本，并在 `BacktestRunSummary` / `get_backtest_run` / `get_latest_backtest_run` / `list_backtest_runs` 返回该字段。
- 扩展 `apps/api/tests/test_backtesting.py`，覆盖 run 创建、读取、列表返回和失败路径下的版本上下文。
- 验证通过：
  - `PYTHONPATH=src python3 -m unittest tests.test_backtesting`
  - `PYTHONPATH=src python3 -m compileall src`
  - `PYTHONPATH=src python3 -m alembic -c alembic.ini upgrade head`

### 完成说明

- `backtest_runs` 现在可以持久化 `rps_definition_version`，回测结果不再只能依赖参数集版本和代码历史做解释。
- Backtest run 在创建时就固定当前批准的 RPS 契约版本，后续执行、失败、读取和列表比较都沿用同一上下文。
- Backtest 与 screen 已共享同一语义版本来源，避免两条链路各维护一套 definition-version 字符串。
- 缺失派生事实的失败路径仍保留已创建的 run 记录，并显式保留其版本上下文，便于后续调查。

### 文件清单

- _bmad-output/implementation-artifacts/5-5-persist-rps-definition-version-with-backtest-runs.md
- _bmad-output/implementation-artifacts/sprint-status.yaml
- apps/api/src/stockanalyse_api/domain/backtests/models.py
- apps/api/src/stockanalyse_api/services/backtesting.py
- apps/api/migrations/versions/20260415_0014_add_backtest_run_rps_definition_version.py
- apps/api/tests/test_backtesting.py

### 变更日志

- 2026-04-14: 创建中文 story，覆盖 `backtest_runs` 的 definition-version 持久化、读取链路和测试要求。
- 2026-04-14: 为 backtest run 落地 `rps_definition_version` 持久化与读取链路，并补充后端回归测试。
