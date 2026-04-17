# 故事 5.6: Anchor Portfolio-Return Backtest Definition

状态: review

## 用户故事

作为产品和工程团队，
我希望 backtest 定义被显式锚定到"portfolio-return 模拟"，并固化 entry / sizing / holding / stop-loss / ranking / benchmark 的边界，
以便未来的 stories、实现和测试用例不会悄悄回退到"historical condition-hit"统计模型。

## 验收标准

1. **AC1（Anchor 文档存在且完备）**：在仓库 `_bmad-output/planning-artifacts/portfolio-backtest-anchor.md`（文件名锁定为 `portfolio-backtest-anchor.md`）下创建一份单一的 anchor 文档，**毫无歧义**地记录以下全部内容：
   - 入场：T+1 open，配套 FR68 的 deferral 与 exclusion 规则；configurable `entry_deferral_window_days`，MVP 默认 = 5 trading days（依 FR71）
   - 仓位：等权 sizing，configurable portfolio cap，MVP 默认 = 20 securities
   - cap exclusion 排序策略：按 RPS composite score 降序，ticker 作为确定性 tie-breaker
   - 持有期：configurable，单位 trading days，MVP 默认 = 20
   - 单证券 stop-loss：configurable，对自身入场价，MVP 默认 = -8%；breach 信号每日由 daily adjusted close 计算一次
   - 平仓（stop-loss 与持有期到期）：在 next valid trading day open 执行，沿用 FR70 的 deferral 规则
   - 单次 backtest 内不允许 rebalance、re-entry 或加仓
   - 释放出来的现金在同一次 backtest 内不再投入
   - 仓位采用 fractional share sizing（MVP 简化），且初始 `portfolio_value` 定义为**无量纲常量 `1.0`**，于是每只证券权重 = `1/N`，所有 portfolio-level return 都是权重比率（不是货币）
   - win-rate 与 maximum-drawdown 定义引用 FR45（normative source），anchor 文档**不重新定义这两个指标**，仅链接
   - MVP 不包含 benchmark 比较
   - 每次 run 持久化以下内容：source `screen_run_id`、parameter snapshot、holding parameters、stop-loss parameters、portfolio cap、`entry_deferral_window_days`、ranking policy identifier、dataset-version identifier、effective default values、`backtest_lifecycle`（默认 `portfolio_return`，依 FR73）
   - 策略定义（RPS contract version、screening parameters）**不**在 backtest record 上独立再存一份，仅通过 `screen_run_id` 引用解析（依 Stories 5.4 和 5.5）

2. **AC2（未来 Story 的引用契约）**：anchor 文档明文写出"任何未来涉及 backtest 执行 / sizing / holding / stop-loss 行为的 story 必须显式引用本 anchor，且任何偏离必须显式声明，而非暗中回退到 condition-hit 模型"，并列出 Stories 5.1–5.5 的语义反向追溯关系（这些 story 的 AC 已显式引用本 anchor）。

3. **AC3（`backtest_lifecycle` 字段持久化）**：在 `apps/api/src/stockanalyse_api/domain/backtests/models.py` 的 `BacktestRun` 模型上新增 `backtest_lifecycle` 列：
   - 类型：`String(32)`，`nullable=False`
   - 取值约束：`portfolio_return` 或 `legacy_condition_hit`，通过 `CheckConstraint` 强制
   - 新增模块级常量 `BACKTEST_LIFECYCLE_VALUES = ("portfolio_return", "legacy_condition_hit")`，沿用现有 `BACKTEST_RUN_STATUS_VALUES` 的命名风格

4. **AC4（迁移 + Backfill）**：新增 Alembic migration `apps/api/migrations/versions/20260417_0017_add_backtest_run_lifecycle.py`：
   - `revision = "20260417_0017"`，`down_revision = "20260415_0016"`
   - upgrade：使用 `op.batch_alter_table("backtest_runs")` 加列（先 nullable=True with server_default），然后 `UPDATE backtest_runs SET backtest_lifecycle = 'legacy_condition_hit' WHERE backtest_lifecycle IS NULL`，最后 `ALTER` 为 `nullable=False`，并加 `CheckConstraint("backtest_lifecycle IN ('portfolio_return', 'legacy_condition_hit')", name="backtest_runs_lifecycle")`
   - downgrade：drop constraint + drop column
   - **既存所有 backtest_runs 行都必须 backfill 为 `legacy_condition_hit`**（因为既存 runs 都是旧 condition-hit 模型实现的，依 FR73 与 Story 5.6 anchor 第 4 项）。**绝不允许**留 NULL。

5. **AC5（service 层默认值）**：`apps/api/src/stockanalyse_api/services/backtesting.py` 中创建 `BacktestRun` 时显式传入 `backtest_lifecycle="portfolio_return"`（不依赖 server_default 隐式默认）；list / get / launch API 响应中暴露此字段。

6. **AC6（前端区分 lifecycle）**：当前前端 backtest UI 只有 `apps/web/src/components/backtests/BacktestLaunchPanel.tsx` 与 `apps/web/src/app/backtests/page.tsx`（无独立 result list / comparison 组件）。在这两个文件中：
   - 渲染 backtest run（list 或 detail）时，给 `legacy_condition_hit` runs 加显式视觉标签（例如徽章 `历史 condition-hit 模型`），与 `portfolio_return` runs **可视化分隔**
   - 即便当前页面尚无组合层聚合卡，也必须在代码中为现有任何"汇总跨多个 run"的展示路径显式过滤 `lifecycle === 'portfolio_return'`，并在 anchor 文档中固化这条 lifecycle 过滤合约（未来新增 result-list / comparison 组件须遵循同一合约）
   - `apps/web/src/lib/types.ts` 增加 `backtest_lifecycle: 'portfolio_return' | 'legacy_condition_hit'` 字段到 backtest run type

7. **AC7（测试覆盖）**：
   - `apps/api/tests/test_backtesting.py` 新增：（a）migration upgrade 后既存 row 的 `backtest_lifecycle` 应为 `legacy_condition_hit`；（b）新建 run 默认 `portfolio_return`；（c）写入非法 lifecycle 值应被 CheckConstraint 拒绝
   - `apps/web/tests/components/BacktestLaunchPanel.test.tsx` 新增：（a）渲染 legacy run 时显示 `legacy_condition_hit` 标签；（b）renders 不把 legacy run 计入 portfolio-return 聚合卡

## 任务 / 子任务

- [x] 编写 anchor 文档（AC: 1, 2）
  - [x] 在 `_bmad-output/planning-artifacts/portfolio-backtest-anchor.md` 创建文档，包含 AC1 列出的全部 12 项规格
  - [x] 在文档底部加"未来 Story 引用契约"小节，明文写出 AC2 的引用规则与 5.1–5.5 反向追溯关系
- [x] 数据模型 + migration（AC: 3, 4）
  - [x] 在 `apps/api/src/stockanalyse_api/domain/backtests/models.py` 新增 `BACKTEST_LIFECYCLE_VALUES` 常量、`backtest_lifecycle` mapped column、`CheckConstraint`
  - [x] 创建 `apps/api/migrations/versions/20260417_0017_add_backtest_run_lifecycle.py`，三步 upgrade（add nullable → backfill → alter to NOT NULL + add constraint），下行 drop constraint + column
- [x] Service 层默认值（AC: 5）
  - [x] `services/backtesting.py` 中 `BacktestRun(...)` 构造显式传 `backtest_lifecycle="portfolio_return"`
  - [x] list / get / launch API 序列化输出 `backtest_lifecycle`
- [x] 前端区分 lifecycle（AC: 6）
  - [x] `apps/web/src/lib/types.ts` 增加 `backtest_lifecycle` 字段到 backtest run type
  - [x] result list / comparison 组件给 `legacy_condition_hit` runs 加视觉标签且与 portfolio-return 区隔
  - [x] 任何组合层聚合卡显式过滤 `lifecycle === 'portfolio_return'`
- [x] 测试（AC: 7）
  - [x] `apps/api/tests/test_backtesting.py` 加 3 个测试（backfill / 默认 / constraint）
  - [x] `apps/web/tests/components/BacktestLaunchPanel.test.tsx` 加 2 个测试（legacy 标签 / 聚合排除）
  - [x] 跑 `PYTHONPATH=src python3 -m unittest tests.test_backtesting`、`PYTHONPATH=src python3 -m alembic -c alembic.ini upgrade head`、`npm run lint`、`npm run build`、`npm run test`

## 开发备注

- **本 story 的核心交付物是 anchor 文档**，代码侧只是为了让"未来 stories 不能悄悄回退到 condition-hit 模型"这条约束在数据层有强制力（通过 `backtest_lifecycle` 字段）。
- **绝对不要**在本 story 中实现新的 portfolio-return 执行引擎（那是后续 5.x 重做的范围）。本 story 只锚定语义、加 lifecycle 字段并 backfill。既存 backtest 执行流程保持不变；新创建的 run 标 `portfolio_return` 是为了让后续 portfolio-return 引擎接管时数据已就位。
- 既存 5.1–5.5 的实现是基于 condition-hit 模型的（参考 done 状态的 `5-1-launch-and-persist-backtest-runs.md` 等文件中的字段：`trade_dates_evaluated`、`qualifying_observations`、`unique_qualified_instruments` 都是 condition-hit 时代的统计），因此 backfill 必须把它们标 `legacy_condition_hit`。
- **不要**在本 story 中重新定义 win rate / max drawdown，它们的 normative source 是 FR45（PRD line 448），anchor 文档仅链接。
- **不要**在 backtest record 上独立再存 RPS contract version / screening parameters；这是 Stories 5.4 和 5.5 已修订的语义（通过 `screen_run_id` 引用解析）。本 story 不触碰这部分。
- Migration 编号沿用 `YYYYMMDD_NNNN` 格式（参考 `20260415_0016_add_strategy_configuration_rps_window_selection.py`），下一个空闲编号是 `20260417_0017`。
- Alembic batch_alter_table 是 SQLite 兼容写法（既存所有 migration 都用 batch_op 加列），必须沿用。
- `BACKTEST_RUN_STATUS_VALUES` 这种"模块级 tuple 常量 + CheckConstraint"是 backtests domain 已有的命名/形态约定（参考 models.py:10），lifecycle 字段必须沿用同一形态。

### Project Structure Notes

- 模型：`apps/api/src/stockanalyse_api/domain/backtests/models.py`
- 迁移：`apps/api/migrations/versions/20260417_0017_add_backtest_run_lifecycle.py`（新建）
- 服务：`apps/api/src/stockanalyse_api/services/backtesting.py:88`（`BacktestRun(...)` 构造调用点）
- 路由：`apps/api/src/stockanalyse_api/api/routes/backtests.py`
- Web 类型：`apps/web/src/lib/types.ts`
- Web 组件：`apps/web/src/components/backtests/BacktestLaunchPanel.tsx`，`apps/web/src/app/backtests/page.tsx`
- Anchor 文档：`_bmad-output/planning-artifacts/portfolio-backtest-anchor.md`（新建，路径锁定）
- 后端测试：`apps/api/tests/test_backtesting.py`
- 前端测试：`apps/web/tests/components/BacktestLaunchPanel.test.tsx`

### References

- Anchor 故事原文：[Source: _bmad-output/planning-artifacts/epics.md#Story 5.6]
- FR68 / FR70 / FR71 / FR73：[Source: _bmad-output/planning-artifacts/prd.md] 与 [Source: _bmad-output/planning-artifacts/epics.md#Requirements Inventory]
- FR45 normative win-rate / max-drawdown 定义：[Source: _bmad-output/planning-artifacts/prd.md:448]
- 既存 backtest schema：[Source: apps/api/src/stockanalyse_api/domain/backtests/models.py]
- Migration 模式参考：[Source: apps/api/migrations/versions/20260415_0014_add_backtest_run_rps_definition_version.py]
- Story 5.4 / 5.5 修订（screen_run_id 引用，不再独立持久化策略定义）：[Source: _bmad-output/planning-artifacts/epics.md#Story 5.4, #Story 5.5]
- Sprint Change Proposal v3 patch：[Source: _bmad-output/planning-artifacts/sprint-change-proposal-2026-04-16-page-review-followups.md]

## 开发代理记录

### 使用的代理模型

gpt-5.4

### 调试日志参考

- 2026-04-17：确认 `sprint-status.yaml` 与 5.1–5.5 story 文档均把 Story 5.6 标为 semantic source，因此优先实现 5.6。
- 2026-04-17：创建 `_bmad-output/planning-artifacts/portfolio-backtest-anchor.md`，锁定 portfolio-return MVP 语义、lifecycle segregation contract，以及 5.1–5.5 的反向追溯关系。
- 2026-04-17：在 `BacktestRun` 模型新增 `backtest_lifecycle` 列与 `BACKTEST_LIFECYCLE_VALUES` 常量，并新增 Alembic migration `20260417_0017` 完成 legacy backfill。
- 2026-04-17：`services/backtesting.py` 显式写入 `backtest_lifecycle="portfolio_return"`，并让 list / get / launch 序列化带出该字段。
- 2026-04-17：前端将 legacy condition-hit runs 与 portfolio-return runs 分开展示，现有跨-run 汇总路径显式只统计 `portfolio_return` lifecycle。
- 2026-04-17：验证通过 `PYTHONPATH=src python3 -m unittest tests.test_backtesting`、`PYTHONPATH=src python3 -m unittest discover tests`、`PYTHONPATH=src python3 -m alembic -c alembic.ini upgrade head`、`npm run lint`、`npm run build`、`npm run test`。

### 完成说明

- 已交付单一 anchor 文档，明确锁定 entry / sizing / holding / stop-loss / ranking / benchmark / provenance / lifecycle segregation 语义，并要求未来 stories 显式引用。
- 已在后端数据层引入 `backtest_lifecycle` 非空字段、枚举约束与 migration backfill；既存 runs 会被标记为 `legacy_condition_hit`，新建 runs 明确标记为 `portfolio_return`。
- 已在前端对 legacy runs 加显式视觉标签，并把现有跨-run 汇总/对比路径限定为只统计 `portfolio_return` lifecycle。
- 已补齐后端与前端测试，覆盖 migration backfill、默认 lifecycle、非法 lifecycle 拒绝、legacy badge 展示、legacy 排除出 portfolio-return 聚合。
- Review follow-up：已修复 `/backtests` 页面将 `visibleRuns` 与面板 `initialRuns` 传参不一致的问题，并新增基于 Alembic migration 产物的非法 lifecycle 约束测试。

### 文件清单

- _bmad-output/planning-artifacts/portfolio-backtest-anchor.md
- apps/api/src/stockanalyse_api/domain/backtests/models.py
- apps/api/migrations/versions/20260417_0017_add_backtest_run_lifecycle.py
- apps/api/src/stockanalyse_api/services/backtesting.py
- apps/api/tests/test_backtesting.py
- apps/web/src/lib/types.ts
- apps/web/src/components/backtests/BacktestLaunchPanel.tsx
- apps/web/src/app/backtests/page.tsx
- apps/web/src/app/globals.css
- apps/web/tests/components/BacktestLaunchPanel.test.tsx

### 变更日志

- 2026-04-17: Story 5.6 创建完成（v3 增量补丁阶段）。Anchor 文档与 lifecycle 字段是后续 5.x portfolio-return 引擎实现的前置依赖。
- 2026-04-17: Story 5.6 开发完成并进入 review。新增 portfolio-return anchor 文档、`backtest_lifecycle` 数据约束、legacy backfill migration、前端 lifecycle 区隔与测试覆盖。
- 2026-04-17: Post-review 修正 `BacktestsPage` 的 `visibleRuns` 传递不一致，并增加 migration 级别的 lifecycle constraint 回归测试。
