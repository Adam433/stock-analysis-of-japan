# 故事 5.5 (portfolio-return 重做): Reference Screen-Run Provenance from Backtest Runs Without Re-Storing Strategy Definition

状态: review

> **本 story 重做了旧 5-5-persist-rps-definition-version-with-backtest-runs（done）**。旧 story 在 backtest record 上独立存 RPS definition version；新 story 反转契约：backtest record 不再独立持有策略定义，**唯一 source-of-truth 是 `source_screen_run_id` 引用**，并显式定义 source 不可达时的降级状态。**Story 5.6 anchor + Story 5.4 共同建立此架构契约**。

## 用户故事

作为 operator，
我希望每个 portfolio-return backtest run 引用其 source screen run 作为策略定义（RPS version、parameter snapshot、stored dataset version）的**单一 source-of-truth**，
以便历史模拟在合约更新后仍可重现可比，且 backtest record 永不与驱动它的 screen run 漂移。

## 验收标准

1. **AC1（持久化策略边界）**：portfolio-return backtest run 持久化时，**只**写：
   - `source_screen_run_id`（FK→screen_runs.id, NOT NULL，对 lifecycle='portfolio_return' 行）
   - 自己的执行模型参数：`effective_holding_days`、`effective_stop_loss_pct`、`effective_portfolio_cap`、`effective_entry_deferral_window_days`、`ranking_policy_id`、`dataset_checksum`（执行时观察）、`backtest_lifecycle='portfolio_return'`
   - **绝不写**：screening 策略定义、RPS contract version、selected_rps_windows、rps_threshold、high_proximity_threshold_pct 等任何 screening 配置（这些归 source screen run 所有）

2. **AC2（渲染时 resolve via screen_run_id）**：任何 result / comparison / operator-investigation 表面渲染 portfolio-return run 时，**或者**通过 `source_screen_run_id` resolve 出策略定义，**或者**显式呈现 source-screen-run-不可用 状态——**不**允许在前端本地拼凑/合成定义。

3. **AC3（source 不可达 → 显式降级）**：source screen run 已被删除 / archived / 不可解析时，UI 显示明确的"原筛选记录不可用 — 策略定义无法解析"状态（与 Story 4.2 的"原筛选记录不可用"文案统一），**不**做静默 partial render。后端对应端点返回 HTTP 410 Gone + `error_code: "source_screen_run_unavailable"`。

4. **AC4（跨 RPS 版本对比可观察）**：两个 portfolio-return run 的 source screen run 跑在不同的 RPS definition version 上时，结果差异可通过它们各自的 `source_screen_run_id` 观察（不需翻 git）；这一可观察性由 Story 5.4 的 trace + compare 端点实现，本 story 提供它**所必需的不可变**契约：每个 screen run 已经持有自己的 `rps_definition_version`，backtest 不再 copy。

5. **AC5（Migration 强一致）**：新增 DB-level constraint 保证 lifecycle='portfolio_return' 的 row 上 `source_screen_run_id IS NOT NULL` 且 `rps_definition_version IS NULL`：
   - SQLite 不支持 partial NOT NULL，因此用 CheckConstraint：`(backtest_lifecycle = 'legacy_condition_hit') OR (backtest_lifecycle = 'portfolio_return' AND source_screen_run_id IS NOT NULL AND rps_definition_version IS NULL)`，命名 `backtest_runs_portfolio_return_provenance`

## 任务 / 子任务

- [x] 数据模型 + migration（AC: 1, 5）
  - [x] 在 `apps/api/src/stockanalyse_api/domain/backtests/models.py` 加 CheckConstraint `backtest_runs_portfolio_return_provenance`，文字按 AC5 定义
  - [x] Migration `20260417_0020_enforce_portfolio_return_provenance_constraint.py`（依赖 0019）
    - 用 batch_alter_table 加 constraint
    - 升级前先验证：所有 lifecycle='portfolio_return' 行已满足条件（5.1 launch 已经写对的话，应该天然满足；如果有不一致行，migration 要么 backfill 要么 fail-fast 报错让人手动修）
    - downgrade 删除 constraint
- [x] Service + 端点契约（AC: 2, 3）
  - [x] 在 `services/portfolio_backtest_traceability.py::resolve_semantics_via_source_screen_run`（5.4 创建）的基础上，加 `resolve_screen_run_or_unavailable(session, run_id) -> dict | None`：FK 解析失败 / row 已删 / status 异常时返回 `None`
  - [x] 各 result / compare / trace 端点统一在 source 不可达时返回 410 Gone + `{"error": "source_screen_run_unavailable", "backtest_run_id": ...}`
  - [x] 5.3 已建的 `/backtests/portfolio-return/runs/{run_id}/result` 端点也走该 fallback——source 不可用时返回 410 而不是 5xx
- [x] 前端降级（AC: 3）
  - [x] `PortfolioReturnResultPanel.tsx`（5.3 已建）：处理 410 响应，渲染 "原筛选记录不可用 — 策略定义无法解析" 文案 + 禁用 trace link
  - [x] `PortfolioReturnComparePanel.tsx`（5.3 已建）：列表中标灰 + 禁用勾选不可用 source 的 run + tooltip
- [x] 反例测试（AC: 1, 5 — disaster prevention）
  - [x] **断言代码 grep**：`apps/api/src/stockanalyse_api/services/portfolio_backtest.py` 中创建 BacktestRun(...) 调用**不**带任何 RPS / screening 字段（用 ast 静态扫或 pytest 文本断言）
  - [x] 写一条 lifecycle='portfolio_return' + source_screen_run_id=NULL 的 row 应被 CheckConstraint 拒
  - [x] 写一条 lifecycle='portfolio_return' + rps_definition_version='X' 的 row 应被 CheckConstraint 拒
- [x] 正向测试（AC: 2, 3, 4）
  - [x] 后端：result / compare / trace 端点对 source 已删的 run 返回 410 + 正确 error_code
  - [x] 前端：result panel / compare panel 在 410 响应下渲染降级 UI；trace link 禁用
  - [x] 跑 `PYTHONPATH=src python3 -m unittest`、`alembic upgrade head`、`npm run lint`、`npm run build`、`npm run test`

## 开发备注

- 这是 **architectural constraint story**（不是新功能）。它的价值在于**用 DB CheckConstraint + service 层守护把"backtest 不能漂移"硬编码进 schema**，使未来误操作（"为方便就在 backtest record 上 cache 一份策略"）在 commit 时被 DB 拒绝。
- 如果迁移时发现既有 lifecycle='portfolio_return' 行违反 constraint，**fail-fast 抛错让人手动审**（不要静默 backfill 把数据"修圆"）——这是 anchor 的精神：宁可挡住 deploy，不允许在策略追溯上含糊。
- 旧 lifecycle='legacy_condition_hit' 行**不**受新 constraint 约束（依 CheckConstraint 公式：legacy 路径直接通过）。它们历史值（含 rps_definition_version 等）保留不动。
- 不引入新的 model 字段；只加 CheckConstraint。Story 5.1 已经加了 source_screen_run_id（nullable=True 兼容 legacy），本 story 通过 CheckConstraint 在 portfolio_return 子集上把它升级为 NOT NULL 等价。
- 410 Gone 是合适状态码：源资源（screen run）确实曾经存在但现在不可达。不要用 404（404 暗示从未存在）。
- "源 screen run 不可用 → 不可解析策略定义"的契约也意味着：portfolio-return run 一旦失去 source，就只剩下 execution-result 数据有意义——可读，但不再可"解释"。这个降级要在 UI 上明确告诉 operator。

### Project Structure Notes

- 模型：`apps/api/src/stockanalyse_api/domain/backtests/models.py`（追加 CheckConstraint）
- 迁移：`apps/api/migrations/versions/20260417_0020_enforce_portfolio_return_provenance_constraint.py`（新建）
- Service：`apps/api/src/stockanalyse_api/services/portfolio_backtest_traceability.py`（5.4 创建，本 story 扩展）
- 路由：`apps/api/src/stockanalyse_api/api/routes/backtests.py`（统一 410 响应分支）
- 前端 panel：`apps/web/src/components/backtests/PortfolioReturnResultPanel.tsx`、`PortfolioReturnComparePanel.tsx`
- 测试：`apps/api/tests/test_backtesting.py`、对应前端 test 文件

### References

- 故事 + AC：[Source: _bmad-output/planning-artifacts/epics.md#Story 5.5]
- Story 5.4 同源契约 + trace 端点：[Source: _bmad-output/planning-artifacts/epics.md#Story 5.4]
- Anchor 第 13 项 + Story 5.1 source_screen_run_id 引入：[Source: _bmad-output/planning-artifacts/portfolio-backtest-anchor.md]、Story 5.1 文件
- 既有 lifecycle 字段：Story 5.6 schema
- "原筛选记录不可用" 文案约定：[Source: _bmad-output/planning-artifacts/epics.md#Story 4.2 AC]

## 开发代理记录

### 使用的代理模型

gpt-5.4

### 调试日志参考

- `PYTHONPATH=src python3 -m unittest tests.test_backtesting`
- `PYTHONPATH=src python3 -m unittest`
- `PYTHONPATH=src python3 -m alembic -c alembic.ini upgrade head`
- `npm run test -- BacktestLaunchPanel.test.tsx PortfolioReturnResultPanel.test.tsx PortfolioReturnComparePanel.test.tsx PortfolioReturnPages.test.tsx WorkflowPages.test.tsx`
- `npm run test`
- `npm run lint`
- `npm run build`

### 完成说明

- 已把 portfolio-return provenance-only 契约固化到 schema：`BacktestRun` 新增 `backtest_runs_portfolio_return_provenance` CheckConstraint，`20260417_0020` migration 在加约束前会先扫出非法 row 并 fail-fast，避免静默“修圆”历史数据。
- 已把 source-screen-run 不可达的降级路径统一到 service 和 route：result / compare / trace 都返回结构化 `410 Gone` + `source_screen_run_unavailable`，`resolve_screen_run_or_unavailable` 成为统一解析入口。
- 已把前端结果页、对比页、以及 launch 面板上的 compare/latest-run 入口都收紧为 provenance-aware UI：source 不可解析时显示统一文案“原筛选记录不可用 — 策略定义无法解析”，并禁用 trace / compare / 结果详情入口，不再 partial render。
- 为满足 5.5 的 schema 契约，旧泛化 `launch_backtest_run` 路径已明确回到 `legacy_condition_hit` lifecycle；新的 `portfolio_return` rows 必须带 `source_screen_run_id` 且不得独立写 `rps_definition_version`。
- 本轮复审唯一修正点是 `BacktestLaunchPanel` 新增第二个禁用入口后，测试仍按单个 “结果不可解释” 断言，现已改为验证两个禁用入口同时存在；实现逻辑本身未回退。

### 文件清单

- apps/api/migrations/versions/20260417_0020_enforce_portfolio_return_provenance_constraint.py
- apps/api/src/stockanalyse_api/api/routes/backtests.py
- apps/api/src/stockanalyse_api/domain/backtests/models.py
- apps/api/src/stockanalyse_api/services/backtesting.py
- apps/api/src/stockanalyse_api/services/portfolio_backtest.py
- apps/api/src/stockanalyse_api/services/portfolio_backtest_traceability.py
- apps/api/tests/test_backtesting.py
- apps/web/src/app/backtests/portfolio-return/[runId]/page.tsx
- apps/web/src/app/backtests/portfolio-return/compare/page.tsx
- apps/web/src/app/globals.css
- apps/web/src/components/backtests/BacktestLaunchPanel.tsx
- apps/web/src/components/backtests/PortfolioReturnComparePanel.tsx
- apps/web/src/components/backtests/PortfolioReturnResultPanel.tsx
- apps/web/src/lib/backtestErrors.ts
- apps/web/src/lib/types.ts
- apps/web/tests/components/BacktestLaunchPanel.test.tsx
- apps/web/tests/components/PortfolioReturnComparePanel.test.tsx
- apps/web/tests/components/PortfolioReturnResultPanel.test.tsx
- apps/web/tests/pages/PortfolioReturnPages.test.tsx

### 变更日志

- 2026-04-17: Story 5.5 portfolio-return 重做版本创建（v3 patch）。Architectural constraint：CheckConstraint 把"backtest 不能漂移"硬编码进 schema。
- 2026-04-17: 完成 5.5 provenance-only 契约开发并进入 review。新增 DB CheckConstraint / `0020` migration、统一的 `410 Gone` source-unavailable 响应、前端降级态与静态防漂移测试。
- 2026-04-17: Post-review 对 `BacktestLaunchPanel` 的 source-unavailable latest-run 区块补齐降级展示，并同步修正测试断言以覆盖两个禁用入口。
