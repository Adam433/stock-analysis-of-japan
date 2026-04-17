# 故事 5.1 (portfolio-return 重做): Launch Portfolio-Return Backtest from a Screen Run as a Single Action

状态: review

> **本 story 重做了旧 5-1-launch-and-persist-backtest-runs（done，condition-hit 模型）**。旧 launch 流程入口是 strategy_configuration_id + 历史日期范围；新流程入口是 `screen_run_id`，并合并 launch + execute 为单一动作。**Story 5.6 anchor 文档（`_bmad-output/planning-artifacts/portfolio-backtest-anchor.md`）是本 story 的 semantic source；本 story 不重新定义 anchor 中已经写明的语义**。

## 用户故事

作为用户，
我希望从一个已完成的 screen run 出发，**单一动作**启动 portfolio-return backtest，
以便我可以评估"如果按这个 screen 的 qualified set 在 T+1 入场会怎样"，而不需要自己去拼后端 task 步骤。

## 验收标准

1. **AC1（单一启动动作）**：给定一个 `status=completed` 的 screen run，给定 backtest 配置表单，当用户点击单一 launch 动作时，系统在一次操作中**同时**创建 backtest_run 记录并开始执行（不暴露独立的 "execute" 动作）。

2. **AC2（执行中状态）**：当执行超过单次请求周期时，UI 暴露**单一** in-progress 状态；MVP 不暴露 cancel 动作。

3. **AC3（默认值引用 anchor）**：当用户**不**覆盖 MVP 默认值时，启动应用 anchor 文档中定义的默认（holding 20 trading days、stop-loss -8%、portfolio cap 20、entry-deferral window 5 trading days）。**本 story 不在代码或测试中重新声明这些数字**——必须从一个集中常量模块读取（见 Dev Notes）。

4. **AC4（参数验证依 FR71）**：用户覆盖默认值时，按 FR71 验证：holding ≥ 1 (integer)、stop-loss ∈ (-1, 0)、cap ≥ 1 (integer)、entry-deferral ≥ 1 (integer)；按 anchor 不允许重新定义这些边界。launch 按钮在验证失败时禁用，并显示 per-field 失败原因。

5. **AC5（debounce）**：用户在首次响应返回前多次点击 launch 时，系统去重，只创建一个 run。

6. **AC6（failed-recoverable）**：persistence 成功但 execution 未启动时，run 标 `failed-recoverable`，用户可从同一记录重试，无需手工清理。

7. **AC7（lifecycle = portfolio_return）**：新创建的 run 必须 `backtest_lifecycle = 'portfolio_return'`（Story 5.6 已完成 schema 与 service 层默认值；本 story 验证 launch 路径不绕过它）。

8. **AC8（来源 screen run 持久化）**：run 记录持久化 `source_screen_run_id`（FK to screen_runs.id, nullable=False, index）+ `effective_holding_days`、`effective_stop_loss_pct`、`effective_portfolio_cap`、`effective_entry_deferral_window_days` 四个生效参数。

## 任务 / 子任务

- [x] 数据模型 + migration（AC: 7, 8）
  - [x] 在 `apps/api/src/stockanalyse_api/domain/backtests/models.py` 加 `source_screen_run_id`（FK→screen_runs.id, nullable=False）、四个 effective 参数列；加 `BACKTEST_RUN_STATUS_VALUES` 中追加 `'failed-recoverable'`（如未存在）
  - [x] 创建 migration `20260417_0018_add_backtest_portfolio_return_launch_fields.py`，依赖 `20260417_0017`；既存 row（lifecycle=`legacy_condition_hit`）的 `source_screen_run_id` 设为 nullable=True 仅 backfill 阶段，对既存 row 写 NULL（旧 condition-hit runs 没有 screen_run 来源），随后 ALTER 为 NOT NULL **仅对 lifecycle=portfolio_return**——若 SQLite/SQLAlchemy 不支持 partial NOT NULL，则保持 nullable=True 但在 service 层强制：lifecycle=portfolio_return 创建时 source_screen_run_id 必填
  - [x] effective 参数对既存 condition-hit row 写 NULL（不适用），新 portfolio-return run 创建时强制传值
- [x] 默认值集中化（AC: 3）
  - [x] 新建 `apps/api/src/stockanalyse_api/services/portfolio_backtest_defaults.py`：定义 `MVP_HOLDING_DAYS = 20`、`MVP_STOP_LOSS_PCT = -0.08`、`MVP_PORTFOLIO_CAP = 20`、`MVP_ENTRY_DEFERRAL_WINDOW_DAYS = 5`；模块顶部 docstring 引用 `_bmad-output/planning-artifacts/portfolio-backtest-anchor.md` 为唯一 normative source
  - [x] 前端 `apps/web/src/lib/portfolio-backtest-defaults.ts` 同名常量，通过后端 `/backtests/defaults` 端点拉取（避免前后端各定一份）
  - [x] 新建 GET `/backtests/defaults` 端点，返回上述四个值
- [x] Launch 服务 + API（AC: 1, 4, 5, 6, 7, 8）
  - [x] 新建 `apps/api/src/stockanalyse_api/services/portfolio_backtest.py::launch_portfolio_return_backtest(session, *, screen_run_id, holding_days?, stop_loss_pct?, portfolio_cap?, entry_deferral_window_days?)`
  - [x] 校验 screen_run 存在、status=completed；校验四个参数（缺省即用 defaults）；创建 BacktestRun(lifecycle='portfolio_return', status='running', source_screen_run_id=...)；在同一调用内派发 execute（同步 or 后台 task，参考 Story 5.2）
  - [x] 失败回滚到 status='failed-recoverable' + error_message
  - [x] 新建 POST `/backtests/portfolio-return/runs`，body: `{screen_run_id, holding_days?, stop_loss_pct?, portfolio_cap?, entry_deferral_window_days?}`
  - [x] HTTP 422 处理 ValueError；HTTP 404 screen_run 不存在
- [x] Debounce（AC: 5）
  - [x] 后端：在 service 层做 5s 内同 screen_run_id 重复 launch 的幂等去重（返回首个 run）
  - [x] 前端：launch 按钮在 onClick 后立即 disabled 直到 response（避免双击）
- [x] 前端 launch panel（AC: 1, 2, 4, 5）
  - [x] 重写 `apps/web/src/components/backtests/BacktestLaunchPanel.tsx` 接受 `screenRunId` prop（不再接收日期范围）；表单字段：holding_days / stop_loss_pct / portfolio_cap / entry_deferral_window_days，初值从 `/backtests/defaults` 拉取
  - [x] 验证按 FR71；错误显示在每个 field 下；launch 按钮 disabled
  - [x] launch 后立即 in-progress 状态（单一），无 separate execute 按钮
- [x] 测试（AC: 1, 3, 4, 5, 6, 7, 8）
  - [x] `apps/api/tests/test_backtesting.py` 加：launch 创建 portfolio_return run；缺省采用 defaults 模块；超出验证范围的 4 个参数被拒；debounce 去重；source_screen_run_id 为非 completed screen_run 时拒绝；failed-recoverable 路径
  - [x] `apps/web/tests/components/BacktestLaunchPanel.test.tsx` 加：缺省值从端点拉取；验证错误显示；launch 按钮 disabled 行为
  - [x] 跑 `PYTHONPATH=src python3 -m unittest tests.test_backtesting`、`alembic upgrade head`、`npm run lint`、`npm run build`、`npm run test`

## 开发备注

- **不要**在本 story 中实现执行引擎（T+1 入场、stop-loss 模拟、equal-weight sizing）——那是 Story 5.2。本 story 只到"创建 run + 触发 execute 调用 + 设置 in-progress"为止。
- 旧 condition-hit `execute_backtest_run`（`services/backtesting.py:137`）保留**不动**——它服务 lifecycle=legacy_condition_hit，但 portfolio-return run 不应该走它。Story 5.2 会在新模块 `portfolio_backtest.py` 加 `execute_portfolio_return_backtest`。launch 内部触发的是 5.2 即将创建的新执行函数；本 story 阶段，它可以是 stub（写入 status='running'，留给 5.2 完成 + 切到 'completed'）。
- `portfolio_backtest_defaults.py` 是**唯一**默认值出处。任何文件再写 `20`、`-0.08`、`5` 字面量都是违反 anchor 的——CR 时 grep 这些字面量。
- screen_run 必须 status='completed'（参考 `domain/screens/models.py:38`）；其它状态拒绝 launch。
- BacktestRun 已有 `strategy_configuration_id`（旧字段），新 portfolio-return run 仍写入它（来自 source screen_run 的 strategy_configuration_id），以避免 FK 不可空冲突。这是过渡兼容，Story 5.5 会把所有策略定义读取改为 follow `source_screen_run_id`。
- 旧 `BacktestRunCreateRequest`（routes/backtests.py:20）保留作 legacy；新增独立 endpoint，不复用。
- Migration 编号递增至 `20260417_0018`。

### Project Structure Notes

- 模型：`apps/api/src/stockanalyse_api/domain/backtests/models.py`
- 默认值：`apps/api/src/stockanalyse_api/services/portfolio_backtest_defaults.py`（新建）
- Service：`apps/api/src/stockanalyse_api/services/portfolio_backtest.py`（新建）
- 路由：`apps/api/src/stockanalyse_api/api/routes/backtests.py`（追加 portfolio-return 端点）
- 迁移：`apps/api/migrations/versions/20260417_0018_add_backtest_portfolio_return_launch_fields.py`（新建）
- Web 默认值：`apps/web/src/lib/portfolio-backtest-defaults.ts`（新建）
- Web panel：`apps/web/src/components/backtests/BacktestLaunchPanel.tsx`（重写）
- Web 路径：`apps/web/src/app/backtests/page.tsx`（接收 `?screen_run_id=` query）

### References

- 故事 + AC：[Source: _bmad-output/planning-artifacts/epics.md#Story 5.1]
- Anchor (semantic source)：[Source: _bmad-output/planning-artifacts/portfolio-backtest-anchor.md] 由 Story 5.6 创建；FR45/FR68/FR70/FR71/FR72/FR73
- 既存 launch 实现（保留为 legacy）：[Source: apps/api/src/stockanalyse_api/services/backtesting.py:79]
- ScreenRun 模型：[Source: apps/api/src/stockanalyse_api/domain/screens/models.py:24]
- 既有 backtests 表 + lifecycle 字段：[Source: apps/api/src/stockanalyse_api/domain/backtests/models.py]（Story 5.6 新增 lifecycle 列）

## 开发代理记录

### 使用的代理模型

GPT-5 Codex（Codex desktop）

### 调试日志参考

- `PYTHONPATH=src python3 -m unittest tests.test_backtesting`
- `PYTHONPATH=src python3 -m alembic -c alembic.ini upgrade head`
- `npm run test -- BacktestLaunchPanel.test.tsx WorkflowPages.test.tsx workflow-smoke.test.tsx apiPaths.test.ts`
- `npm run lint`
- `npm run build`

### 完成说明

- 新增 portfolio-return launch defaults/service/API：从 completed `screen_run_id` 单动作创建并派发回测，持久化 `source_screen_run_id` 与四个 effective 参数，并支持 5 秒 debounce 与 `failed-recoverable` 同记录重试。
- 新增 `20260417_0018` migration，并采用重建表方式在 SQLite 上稳定引入 launch 字段与 `failed-recoverable` 状态约束；既有 legacy condition-hit rows 保持 `NULL` provenance/effective 参数。
- 重写 `/backtests` 页面与 `BacktestLaunchPanel`：支持 `?screen_run_id=`，无 query 时回退到 latest screen run；表单默认值通过 `/backtests/defaults` 拉取，按 FR71 做前端校验，launch 后仅保留单一 in-progress 状态，同时保留 5.6 的 lifecycle 分流与 legacy 展示。

### 文件清单

- apps/api/src/stockanalyse_api/domain/backtests/models.py
- apps/api/src/stockanalyse_api/services/backtesting.py
- apps/api/src/stockanalyse_api/services/portfolio_backtest.py
- apps/api/src/stockanalyse_api/services/portfolio_backtest_defaults.py
- apps/api/src/stockanalyse_api/api/routes/backtests.py
- apps/api/migrations/versions/20260417_0018_add_backtest_portfolio_return_launch_fields.py
- apps/api/tests/test_backtesting.py
- apps/web/src/app/backtests/page.tsx
- apps/web/src/components/backtests/BacktestLaunchPanel.tsx
- apps/web/src/lib/apiPaths.ts
- apps/web/src/lib/portfolio-backtest-defaults.ts
- apps/web/src/lib/types.ts
- apps/web/tests/components/BacktestLaunchPanel.test.tsx
- apps/web/tests/e2e/workflow-smoke.test.tsx
- apps/web/tests/lib/apiPaths.test.ts
- apps/web/tests/pages/WorkflowPages.test.tsx

### 变更日志

- 2026-04-17: Story 5.1 portfolio-return 重做版本创建（v3 patch）。依赖 Story 5.6 anchor 文档与 lifecycle 字段；执行引擎在 Story 5.2。
- 2026-04-17: 完成 portfolio-return launch defaults/API/service、BacktestRun provenance/effective 字段、`failed-recoverable` 状态与 `/backtests` 新 screen-run 发起入口；验证通过 unittest、alembic、vitest、lint 与 build。
