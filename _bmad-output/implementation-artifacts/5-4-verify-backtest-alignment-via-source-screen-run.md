# 故事 5.4 (portfolio-return 重做): Verify Backtest Alignment with Approved RPS Semantics via Source Screen Run

状态: ready-for-dev

> **本 story 重做了旧 5-4-verify-backtest-alignment-with-approved-rps-semantics（done）**。旧 story 的检查路径假设 backtest record 自己存了 RPS definition version；新 story 把单一 source-of-truth 锁定为 `source_screen_run_id`：backtest 的 RPS 语义可验证性**完全**通过 trace 回到 source screen run 实现。**Story 5.6 anchor + Story 5.5 共同建立此架构契约**。

## 用户故事

作为用户，
我希望任何 portfolio-return backtest run 的 RPS 语义都可以通过追溯到它的 source screen run 来验证，
以便历史评估永远不会偏离 screening 与 chart 工作流，并且系统不会在 backtest 侧维护一份并行的 RPS 定义。

## 验收标准

1. **AC1（traceability 端点）**：给定一个 portfolio-return backtest run，调 `/backtests/portfolio-return/runs/{run_id}/trace`，返回：
   - `source_screen_run_id`、screen run 的 `trade_date`、`status`、`strategy_configuration_id`、`strategy_configuration.version`、`rps_definition_version`
   - **`backtest_run.rps_definition_version` 字段在响应中显式标 `null` 或省略**——告诉调用方"backtest record 不再独立持有此值，请从 source screen run 读取"

2. **AC2（不再独立写 rps_definition_version）**：portfolio-return launch 流程（Story 5.1 的 `launch_portfolio_return_backtest`）**不**写 `backtest_runs.rps_definition_version`（保持 NULL）。仅 legacy condition-hit 旧路径继续写。lifecycle='portfolio_return' 的 row 上此列必须为 NULL，由 service 层主动验证。

3. **AC3（语义一致性投影）**：调 `/backtests/portfolio-return/runs/{run_id}/semantics-snapshot`，返回的 RPS semantic version + thresholds 等同于"从 source_screen_run_id 读 screen_run.rps_definition_version + screen_run.strategy_configuration（active 版本快照）"得到的值。**不**允许 backtest 侧独立计算或重组定义。

4. **AC4（投资问题可追溯）**：当 portfolio-return run 结果可疑时，operator 可单步从 `source_screen_run_id` 反查到 RPS definition version + parameter snapshot + dataset version，**不需要看代码**。该端点（trace）必须把这三项都包在响应里。

5. **AC5（跨 run 对比的语义差异 surface）**：两个 portfolio-return run 的 source screen run 跑在不同的 RPS definition version 上时，Story 5.3 的 compare 端点返回的对比维度中**显式**包含 `rps_definition_version`（resolve from source_screen_run），且不同时**视觉高亮**——operator 不需翻 git 历史。

## 任务 / 子任务

- [ ] 端点（AC: 1, 3, 4）
  - [ ] 在 `apps/api/src/stockanalyse_api/api/routes/backtests.py` 加 GET `/backtests/portfolio-return/runs/{run_id}/trace`，仅接受 lifecycle='portfolio_return'，否则 422
  - [ ] 加 GET `/backtests/portfolio-return/runs/{run_id}/semantics-snapshot`：返回从 source screen run 与其 strategy_configuration 读到的 RPS semantic version + thresholds + selected_rps_windows + min_rps_lines_required
- [ ] Service 层（AC: 2, 3）
  - [ ] 在 `services/portfolio_backtest.py::launch_portfolio_return_backtest` 中**断言**新 BacktestRun 实例 `rps_definition_version is None`；测试反向：试图在 portfolio-return launch 时显式传 RPS version 应被拒绝（422）
  - [ ] 新建 `services/portfolio_backtest_traceability.py::resolve_semantics_via_source_screen_run(session, run_id) -> dict`：通过 `source_screen_run_id` join `screen_runs` + `strategy_configurations`，返回 RPS semantic 字典；run 的 lifecycle 不是 portfolio_return 时抛 ValueError
- [ ] Compare 端点扩展（AC: 5）
  - [ ] 修订 Story 5.3 已建的 `/backtests/portfolio-return/runs/compare`，对每个 run 多附带 `rps_definition_version` 字段（来自 traceability resolver）
  - [ ] 前端 `PortfolioReturnComparePanel.tsx`：当对比的多 run 之间 `rps_definition_version` 不同时，显式高亮（例如带 ⚠️ 颜色徽章 + tooltip "RPS 定义版本不同"）
- [ ] 测试（AC: 1-5 每个 AC ≥ 1 条）
  - [ ] 后端：trace 端点返回 source screen run 完整 trace；semantics-snapshot 与"读 source screen run 的快照"完全等价；尝试为 portfolio-return run 写非 NULL `rps_definition_version` 应被拒；legacy lifecycle 调 trace 应 422；compare 多 run 不同 RPS version 时响应包含每行 version 字段
  - [ ] 前端：compare panel 多 run 不同 RPS version 时高亮可见；同 version 时不高亮
  - [ ] 跑 `PYTHONPATH=src python3 -m unittest tests.test_backtesting`、`npm run test`

## 开发备注

- 旧 schema 的 `backtest_runs.rps_definition_version`（migration `20260415_0014`）**不要删除**——它服务 lifecycle='legacy_condition_hit' 的旧 run（旧 row 上有值，是历史事实）。本 story 只**约定**新 portfolio-return run 不写它。CR 检查：portfolio_return run 在数据库中此列必须是 NULL。
- 不在 backtest record 上加任何"copy of strategy params"字段。任何想引入"为方便就地展示"的 denormalize 都违反 anchor 第 13 项 + Story 5.5 的契约。
- traceability resolver 的实现路径：`run.source_screen_run_id` → `select(ScreenRun).where(id=...)` → `select(StrategyConfiguration).where(id=screen_run.strategy_configuration_id)`。strategy_configuration 行可能已 inactive（`is_active=False`），仍然返回（因为它代表 screen run 当时的快照状态）。
- 如果 source screen run 已被删除（`source_screen_run_id` FK 仍存在但记录消失——理论上不应该，因为 FK ondelete=RESTRICT 保护；但保险），trace 端点返回 410 Gone + `"source_screen_run_unavailable"`。这是 Story 5.5 主负责的状态码，本 story 沿用即可。
- compare 端点本来在 Story 5.3 已建；本 story 仅"扩展"它的响应字段——避免在多个 story 重复实现。

### Project Structure Notes

- 路由：`apps/api/src/stockanalyse_api/api/routes/backtests.py`（追加 trace + semantics-snapshot 端点）
- Service：`apps/api/src/stockanalyse_api/services/portfolio_backtest_traceability.py`（新建）
- Service：`apps/api/src/stockanalyse_api/services/portfolio_backtest.py`（5.1 已建；本 story 加 launch 时 rps_definition_version 防写断言）
- 前端 compare panel 修订：`apps/web/src/components/backtests/PortfolioReturnComparePanel.tsx`（5.3 已建）
- 测试：`apps/api/tests/test_backtesting.py`、`apps/web/tests/components/PortfolioReturnComparePanel.test.tsx`

### References

- 故事 + AC：[Source: _bmad-output/planning-artifacts/epics.md#Story 5.4]
- Story 5.5 同源契约：[Source: _bmad-output/planning-artifacts/epics.md#Story 5.5]
- Anchor 第 13 项（"策略定义不在 backtest record 独立再存"）：[Source: _bmad-output/planning-artifacts/portfolio-backtest-anchor.md]（Story 5.6）
- ScreenRun.rps_definition_version：[Source: apps/api/src/stockanalyse_api/domain/screens/models.py:35]
- StrategyConfiguration：[Source: apps/api/src/stockanalyse_api/domain/screens/models.py:12]
- 既有 backtest rps_definition_version 字段（保留为 legacy 写入路径）：[Source: apps/api/migrations/versions/20260415_0014_add_backtest_run_rps_definition_version.py]

## 开发代理记录

### 使用的代理模型

{{agent_model_name_version}}

### 调试日志参考

### 完成说明

### 文件清单

### 变更日志

- 2026-04-17: Story 5.4 portfolio-return 重做版本创建（v3 patch）。RPS 语义可验证性单一 source = source_screen_run_id。
