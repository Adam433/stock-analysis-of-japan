# 故事 5.3 (portfolio-return 重做): Review Portfolio-Return Backtest Results and Compare Strategy Adjustments

状态: review

> **本 story 重做了旧 5-3-review-backtest-results-and-compare-strategy-adjustments（done，condition-hit 模型）**。旧 review 展示 qualifying observations 计数；新 review 展示组合层 cumulative return / win rate / max drawdown / equity curve / per-security distribution，并提供跨 run 的对比（sharing dimensions: holding period / stop-loss / portfolio cap / source screen run）。**Story 5.6 anchor 是本 story 的 semantic source；win-rate 与 max-drawdown 引用 FR45（normative），本 story 不重新定义**。

## 用户故事

作为用户，
我希望以"投资视角"打开已完成的 portfolio-return backtest 结果（cumulative return、win rate、max drawdown、equity curve、per-security 分布），并能在我实际调整的维度上**横向对比多个 run**，
以便基于证据迭代策略，而不是仅凭直觉。

## 验收标准

1. **AC1（结果详情视图）**：给定一个 `status='completed'` 且 `lifecycle='portfolio_return'` 的 backtest run，当用户打开结果页时，UI 展示：
   - 组合 cumulative return（百分比）
   - **win rate**（依 FR45 normative 定义：closed positions 中 `realized_return > 0` 的占比）
   - **max drawdown**（依 FR45 normative 定义：portfolio equity curve 的最大 peak-to-trough 降幅）
   - portfolio equity curve（每日组合权益序列折线图）
   - per-security 收益分布（柱状或散点：x=symbol, y=realized_return；含 exit_reason 颜色编码：`stop_loss` / `holding_expired`）

2. **AC2（多 run 对比第一类维度）**：给定多个 lifecycle='portfolio_return' 的 completed run，当用户横向对比时，UI 把以下作为**第一类对比维度**显式呈现（不要藏在折叠里）：
   - holding_days
   - stop_loss_pct
   - portfolio_cap
   - source_screen_run_id（含其 trade_date 与 strategy_configuration.version）

3. **AC3（trade-day 对齐对比）**：当对比的两条 run 来自不同 trade_date 的 source screen run 时，equity curve 不可按 calendar date 对齐（会引入巧合时点偏差），必须按 "trading days since T+1 entry"（x 轴 0 = entry 日）对齐展示。

4. **AC4（lifecycle 隔离）**：legacy `lifecycle='legacy_condition_hit'` runs 在 list 与 comparison view 中**显式标签 + 视觉隔离**（沿用 Story 5.6 已建的视觉标签合约），**绝对不混入** portfolio-return 聚合统计与对比 chart 中。

5. **AC5（trace-back 链接）**：每个 run 详情页有一个明显的 link 跳到其 source screen run（无需复制粘贴 ID）；source screen run 不可达时显示"原筛选记录不可用"的明确状态（沿用 Story 4.2 同款文案）。

## 任务 / 子任务

- [x] API（AC: 1, 2, 3, 4, 5）
  - [x] 在 `apps/api/src/stockanalyse_api/api/routes/backtests.py` 加 GET `/backtests/portfolio-return/runs/{run_id}/result`，返回：cumulative_return、win_rate、max_drawdown、equity_curve、per_security_returns、source_screen_run（id, trade_date, strategy_configuration_version, status）
  - [x] 加 GET `/backtests/portfolio-return/runs/compare?ids=1,2,3`，返回每个 run 的对比维度 + aligned equity curves（按 "days since T+1" 对齐 — 后端预对齐，前端只画图）
  - [x] 两个端点都拒绝 `lifecycle != 'portfolio_return'` 的 run（返回 422）
  - [x] win_rate / max_drawdown 计算函数放 `services/portfolio_backtest_metrics.py`，模块 docstring 引用 FR45
- [x] Web 结果详情页（AC: 1, 5）
  - [x] 新建 `apps/web/src/app/backtests/portfolio-return/[runId]/page.tsx`
  - [x] 新建 `apps/web/src/components/backtests/PortfolioReturnResultPanel.tsx`：展示 cumulative_return / win_rate / max_drawdown 数字卡 + lightweight-charts 折线（equity curve）+ 简单 per-security 分布列表/柱状（参考已有 `lib/types.ts` 复用既有 chart 组件）
  - [x] source screen run trace-back link 到 `/screen?run_id=` 或对应 detail（参考 Epic 2 stories）；不可达走"原筛选记录不可用"
- [x] Web 对比视图（AC: 2, 3, 4）
  - [x] 新建 `apps/web/src/app/backtests/portfolio-return/compare/page.tsx`，query: `?ids=1,2,3`
  - [x] 新建 `apps/web/src/components/backtests/PortfolioReturnComparePanel.tsx`：第一类维度表 + 对齐 equity curves multi-line
  - [x] legacy run 视觉隔离合约：从 list 页面进入 compare 时，UI 拒绝把 lifecycle='legacy_condition_hit' 的 run 加入 selection（显示禁用 + tooltip）
- [x] 后端测试
  - [x] `apps/api/tests/test_backtesting.py` 加：result 端点对 portfolio_return run 返回 5 项；对 legacy run 返回 422；compare 端点对齐 days_since_entry；win_rate / max_drawdown 计算单元测试（含 win_rate=0 / 全胜 / N=0 / 单点 equity curve drawdown=0 等边界）
- [x] 前端测试
  - [x] 新建 `apps/web/tests/components/PortfolioReturnResultPanel.test.tsx`：渲染所有 5 项；空 portfolio（N=0）渲染说明文案；source screen run 不可达走 fallback
  - [x] 新建 `apps/web/tests/components/PortfolioReturnComparePanel.test.tsx`：第一类维度可见；equity curve x 轴是 "days since T+1"；legacy run 不可加入对比
  - [x] 跑 `npm run lint`、`npm run build`、`npm run test`

## 开发备注

- **本 story 不重新定义** win-rate / max-drawdown 的语义。FR45（PRD line 448）是 normative source；后端计算函数模块 docstring 必须**引用** FR45，**不要**在 docstring 中重复语义文字。
- equity curve 的 trade_date 列从 Story 5.2 写入的 `equity_curve_json` 解码；不要在本 story 重新模拟。
- per-security 分布的数据来自 Story 5.2 写入的 `per_security_returns_json`；不要在本 story 重新计算。
- `cumulative_return` 同样由 Story 5.2 已写入；本 story 仅显示。
- "trading days since T+1 entry" 对齐：以 `equity_curve_json` 中的 trade_date 序列减去 entry trade_date，转换为 trading days 索引（0, 1, 2, ...）。trading days = market open days；周末/节假日已经被 5.2 的 indicator 表自然过滤。
- 不使用 calendar-date 对齐——这是 anchor 第 5 项 + AC3 的硬性约束。CR 检查：grep compare panel 中是否有 calendar date 拼接，应为零。
- legacy 隔离合约具体表现：list 中 legacy run badge + 不可勾选进入 compare（disabled checkbox）；不允许"对比 portfolio_return 与 condition-hit"——它们在度量定义上不可比。
- Lightweight-charts 已在 Stock Detail 用过（参考 `apps/web/src/components/stocks/StockDetailCharts.tsx`），equity curve 复用其 line series；不引入新 chart lib。

### Project Structure Notes

- 后端 routes：`apps/api/src/stockanalyse_api/api/routes/backtests.py`
- 后端 metrics：`apps/api/src/stockanalyse_api/services/portfolio_backtest_metrics.py`（新建）
- Web 路由：`apps/web/src/app/backtests/portfolio-return/[runId]/page.tsx`、`apps/web/src/app/backtests/portfolio-return/compare/page.tsx`（新建）
- Web 组件：`apps/web/src/components/backtests/PortfolioReturnResultPanel.tsx`、`PortfolioReturnComparePanel.tsx`（新建）
- 前端 types：`apps/web/src/lib/types.ts`（追加 PortfolioReturnRunResult / Comparison 类型）
- 测试：`apps/api/tests/test_backtesting.py`、`apps/web/tests/components/PortfolioReturnResultPanel.test.tsx`、`PortfolioReturnComparePanel.test.tsx`（新建）

### References

- 故事 + AC：[Source: _bmad-output/planning-artifacts/epics.md#Story 5.3]
- FR45 normative win-rate / max-drawdown：[Source: _bmad-output/planning-artifacts/prd.md:448]
- Anchor 第 5、10、12 项：[Source: _bmad-output/planning-artifacts/portfolio-backtest-anchor.md]（Story 5.6 创建）
- 既有 chart 组件复用：[Source: apps/web/src/components/stocks/StockDetailCharts.tsx]
- "原筛选记录不可用" 文案约定：[Source: _bmad-output/planning-artifacts/epics.md#Story 4.2 AC]

## 开发代理记录

### 使用的代理模型

GPT-5 Codex（Codex desktop）

### 调试日志参考

- `PYTHONPATH=src python3 -m unittest tests.test_backtesting`
- `PYTHONPATH=src python3 -m alembic -c alembic.ini upgrade head`
- `npm run lint`
- `npm run test`
- `npm run build`

### 完成说明

- 新增 FR45 指标接口与结果读取路径：后端现在可以为 completed `portfolio_return` run 返回 cumulative return、win rate、max drawdown、equity curve、per-security returns，以及来源 screen run 摘要；compare 接口会把不同 trade_date 的曲线按 `days_since_entry` 对齐后返回。
- 前端新增结果详情页与 compare 页，并在 `BacktestLaunchPanel` 上补了 compare 选择入口；legacy condition-hit runs 会显式挂牌且禁止加入 portfolio-return 对比。
- 本轮 review 额外补通了 trace-back：结果页跳转到 `/screen?run_id=` 后，`ScreenConfigurationPage` 现在会实际加载指定的 screen run，而不是回退成 generic latest run。

### 文件清单

- apps/api/src/stockanalyse_api/api/routes/backtests.py
- apps/api/src/stockanalyse_api/services/portfolio_backtest_metrics.py
- apps/api/tests/test_backtesting.py
- apps/web/src/app/backtests/portfolio-return/[runId]/page.tsx
- apps/web/src/app/backtests/portfolio-return/compare/page.tsx
- apps/web/src/app/screen/page.tsx
- apps/web/src/app/globals.css
- apps/web/src/components/backtests/BacktestLaunchPanel.tsx
- apps/web/src/components/backtests/PortfolioReturnComparePanel.tsx
- apps/web/src/components/backtests/PortfolioReturnResultPanel.tsx
- apps/web/src/lib/apiPaths.ts
- apps/web/src/lib/formatters.ts
- apps/web/src/lib/types.ts
- apps/web/tests/components/BacktestLaunchPanel.test.tsx
- apps/web/tests/components/PortfolioReturnComparePanel.test.tsx
- apps/web/tests/components/PortfolioReturnResultPanel.test.tsx
- apps/web/tests/lib/apiPaths.test.ts
- apps/web/tests/pages/WorkflowPages.test.tsx

### 变更日志

- 2026-04-17: Story 5.3 portfolio-return 重做版本创建（v3 patch）。
- 2026-04-17: 完成 portfolio-return 结果详情页、compare 页、FR45 metrics 接口与 lifecycle 隔离；review 后补通 source screen run trace-back 到 `run_id` 指定加载路径。
