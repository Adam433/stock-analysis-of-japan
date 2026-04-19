# 故事 5.2 (portfolio-return 重做): Execute Portfolio-Return Backtest with Entry, Holding, and Stop-Loss Rules

状态: review

> **本 story 重做了旧 5-2-execute-reproducible-backtests-from-stored-inputs（done，condition-hit 模型）**。旧执行做"计数 qualifying observations"；新执行做"模拟一个 portfolio：T+1 入场、equal-weight 等权、stop-loss 触发平仓、holding 到期平仓、不再投入释放现金"。**Story 5.6 anchor (`_bmad-output/planning-artifacts/portfolio-backtest-anchor.md`) 是本 story 的 semantic source；本 story 不重新定义 anchor 中已经写明的语义**。

## 用户故事

作为用户，
我希望 backtest 在显式的 entry / holding / stop-loss 规则下，**模拟 screen run qualified set 的后续组合表现**，
以便结果告诉我"这个 screen 作为策略表现如何"，而不是"它的条件历史命中了多少次"。

## 验收标准

1. **AC1（T+1 等权入场）**：给定 `(source_screen_run_id, holding_days, stop_loss_pct, portfolio_cap, entry_deferral_window_days)`，执行 backtest 时在 screen trade_date 的下一个 trading day（T+1）开盘价**等权**入场 qualified securities。

2. **AC2（cap 排序 + 排除）**：qualified set 超过 `portfolio_cap` 时，按 RPS composite score 降序保留前 N，ticker (instrument.symbol) 升序作为确定性 tie-breaker；run 上记录 `ranking_policy_id = 'rps_desc_ticker_asc_v1'` 与被排除 securities 列表（`excluded_securities` JSON：`[{instrument_id, symbol, exclusion_reason: 'cap_overflow'}]`）。

3. **AC3（entry deferral）**：T+1 不是 trading day 或 T+1 open 不可得时，递延到下一个有效 trading day open，**最多** `entry_deferral_window_days` trading days；窗口内仍无有效 open 则该 security 排除（exclusion_reason: `no_valid_open_in_deferral_window`）。

4. **AC4（停牌/退市/公司行动排除）**：security 在整个 deferral 窗口内停牌、退市或发生使 open 不可交易的公司行动时，排除（exclusion_reason: `suspended_delisted_or_corp_action_in_deferral_window`）。

5. **AC5（fractional sizing + portfolio_value 常量）**：N 只 qualified（排除后）等权时，每只权重 `portfolio_value / N`，`portfolio_value = 1.0` 无量纲常量（anchor 已定义；本 story 直接用 `MVP_PORTFOLIO_VALUE = 1.0` 常量，不重新定义）。N=0 时返回完整空 portfolio 结果，**不**抛错。

6. **AC6（stop-loss 信号 + 平仓）**：每个 trading day 用当日 daily adjusted close 计算 stop-loss 信号（breach = `(close / entry_price - 1) <= stop_loss_pct`）；breach 当天结束后，下一个有效 trading day 的 open 价平仓（**包括** gap-down open 价低于 stop-loss 阈值的情况，仍按当日 open 价成交，不做更优执行假设）；下一个 trading day 仍是 halt 或 open 不可得时，再递延到下一个有效 open；释放现金**不**在本 backtest 内再投入。

7. **AC7（持有期到期平仓）**：到期（trading days 计数）后任何仍持仓的 position 在下一个 trading day open 平仓，复用 AC6 的递延规则。

8. **AC8（数据不足以完成持有期）**：持有期会延伸到执行时刻数据中尚未存在的 trading day 时，run 状态返回为 `failed-data-insufficient` + `error_message = "数据不足以完成持有期"`，**不**截断持有期，**不**标 completed。

9. **AC9（无加仓 / 无再入场 / 无 rebalance）**：模拟代码路径中**不存在**任何在 T+1 入场后追加 position / 再入场 / 调整权重的逻辑。

10. **AC10（确定性可重现）**：相同 `(source_screen_run_id, holding_days, stop_loss_pct, portfolio_cap, entry_deferral_window_days, dataset-version identifier)` 第二次执行结果与第一次完全一致；底层 market data 自上次后被 corrected 时，新 run 上 `dataset-version identifier` 变化必须显式地 surfaced 在 run 上（`dataset_checksum` 不同），**不**静默给出不同结果。

11. **AC11（lifecycle = portfolio_return）**：执行写入的 run record `backtest_lifecycle = 'portfolio_return'`（依赖 Story 5.6 + 5.1 已经写入；本 story 不允许覆盖此值）。

## 任务 / 子任务

- [x] 数据模型 + migration（AC: 2, 5, 6, 7, 8, 10）
  - [x] 在 `apps/api/src/stockanalyse_api/domain/backtests/models.py` 加：
    - `ranking_policy_id`（String(64)）
    - `excluded_securities_json`（Text）— 序列化 JSON
    - `portfolio_value` (Numeric(18,6), default 1.0)
    - `position_count_after_exclusions`（Integer）
    - `cumulative_return`（Numeric(18,6), nullable）— 由 5.3 review 用，本 story 写入
    - `equity_curve_json`（Text, nullable）— 序列化每日组合权益序列 `[{trade_date, equity}]`
    - `per_security_returns_json`（Text, nullable）— `[{instrument_id, symbol, entry_date, exit_date, exit_reason, realized_return}]`
  - [x] 新增 `'failed-data-insufficient'`、`'failed-recoverable'` 到 `BACKTEST_RUN_STATUS_VALUES`
  - [x] Migration `20260417_0019_add_portfolio_return_execution_fields.py`，依赖 `0018`；既存 row 全部为 NULL/默认（不影响 condition-hit）；CheckConstraint `status IN (...)` 更新
- [x] 执行引擎（AC: 1-11）
  - [x] 新建 `apps/api/src/stockanalyse_api/services/portfolio_backtest.py::execute_portfolio_return_backtest(session, run_id)`
  - [x] 加载 `screen_run_results`（passed=true）作为 qualified set
  - [x] 加载 `market_data_daily` 在 `[trade_date, trade_date + holding_days + deferral_window]` 范围
  - [x] 实现 cap 排序（RPS composite score 来源：screen_run_results.best_rps_value 降序，instrument.symbol 升序）+ exclusion 记录
  - [x] 实现 deferral 窗口入场：从 trade_date+1 起逐日找第一个有效 open；超窗口排除
  - [x] 等权 sizing：weight = MVP_PORTFOLIO_VALUE / N（N 是排除后数量）
  - [x] 持仓循环：每日 close 检查 stop-loss → 标记 breach；breach 次日 open 平仓（含递延）；持有期到期则到期后下一日 open 平仓
  - [x] 写 equity_curve_json、per_security_returns_json、cumulative_return
  - [x] 计算 dataset_checksum（沿用既有方式：trade_date + instrument 元组 SHA256）
  - [x] 异常分流：数据不足 → `failed-data-insufficient`；其它已知 ValueError → `failed-recoverable`
- [x] 常量
  - [x] `services/portfolio_backtest_defaults.py` 加 `MVP_PORTFOLIO_VALUE = 1.0`（与 holding/stop-loss/cap/deferral 同模块）
- [x] API
  - [x] 在 launch 端点（Story 5.1 已建）内部触发 execute（不暴露独立 execute 端点 — 依 anchor 第 8 项 + Story 5.1 AC1）
- [x] 测试（AC: 1-11，每个 AC 至少 1 条断言）
  - [x] `apps/api/tests/test_backtesting.py` 新增 fixtures：seed screen_run + screen_run_results（passed） + market_data_daily 完整 trade_date 序列
  - [x] 测试覆盖：T+1 入场 / cap 截断 + ranking + excluded 列表 / deferral 入场 / 超 deferral 窗口排除 / suspended 排除 / fractional weight 1/N / N=0 空 portfolio / stop-loss 触发 + 次日 open 平仓 + gap-down 不优化 / 平仓递延 / 释放现金不再投入 / 持有期到期 + 递延 / 数据不足 → failed-data-insufficient / 无加仓路径（grep 断言）/ 重复执行确定性（同输入两次 result 一致）/ dataset-version 变化 surfaced
  - [x] 跑 `PYTHONPATH=src python3 -m unittest tests.test_backtesting`、`alembic upgrade head`

## 开发备注

- **本 story 完全不动旧 condition-hit 路径**（`services/backtesting.py` 中 `execute_backtest_run` 与 `_serialize` 中 condition-hit 字段保留）。新执行函数完全独立。
- portfolio_value=1.0 + 等权 1/N 意味着 portfolio cumulative return 是 sum of `weight × per_security_return`，全部以比率呈现，不携货币单位。
- ranking_policy_id 用字符串而非 enum，便于未来多版本（`rps_desc_ticker_asc_v1` → `_v2`）。
- excluded_securities 用 JSON Text（不引入 JSON column 类型）以保 SQLite 兼容。
- `screen_run_results.best_rps_value` 已经存在（参考 screens/models.py:60），本 story 的 RPS composite score 直接用它（不重新计算）。
- AC8 状态名 `failed-data-insufficient` 与 `failed-recoverable` 都是新值；CHK constraint 必须 ALTER 更新（batch_alter_table 重建表）。
- AC10 的"dataset-version 变化 surfaced"具体含义：dataset_checksum 不同时，run record 仍正常返回，但 result 与上次不同——UI 应**比较 dataset_checksum 区分**（这部分 UI 逻辑在 Story 5.3 review）。
- Stop-loss 触发的"breach 当天计算 / 次日 open 成交"是为反映现实日级别 backtest（无法在当日 close 后真实成交）。gap-down 不优化是反"未来函数"。
- 不实现"重新调入"、"加仓"、"换仓"——CR 时 grep `add_position` / `rebalance` / `re_entry` 等关键词应为零。

### Project Structure Notes

- 模型：`apps/api/src/stockanalyse_api/domain/backtests/models.py`
- Service：`apps/api/src/stockanalyse_api/services/portfolio_backtest.py`（继续 Story 5.1 同一模块）
- 默认值：`apps/api/src/stockanalyse_api/services/portfolio_backtest_defaults.py`（追加 MVP_PORTFOLIO_VALUE）
- 迁移：`apps/api/migrations/versions/20260417_0019_add_portfolio_return_execution_fields.py`
- 测试：`apps/api/tests/test_backtesting.py`
- Market data 来源：`apps/api/src/stockanalyse_api/domain/market_data/`（参考 既有 ingest/normalize）

### References

- 故事 + AC：[Source: _bmad-output/planning-artifacts/epics.md#Story 5.2]
- Anchor（semantic source，含所有 12 项规格）：[Source: _bmad-output/planning-artifacts/portfolio-backtest-anchor.md]（Story 5.6 创建）
- FR68 / FR69 / FR70 / FR71：[Source: _bmad-output/planning-artifacts/prd.md, _bmad-output/planning-artifacts/epics.md#Requirements Inventory]
- ScreenRun + ScreenRunResult：[Source: apps/api/src/stockanalyse_api/domain/screens/models.py]
- 既存 backtest 执行（保留为 legacy condition-hit）：[Source: apps/api/src/stockanalyse_api/services/backtesting.py:137]
- BacktestRun lifecycle 字段：Story 5.6 schema

## 开发代理记录

### 使用的代理模型

GPT-5 Codex（Codex desktop）

### 调试日志参考

- `PYTHONPATH=src python3 -m unittest tests.test_backtesting`
- `PYTHONPATH=src python3 -m alembic -c alembic.ini upgrade head`

### 完成说明

- 新增 `execute_portfolio_return_backtest`，实现 T+1/递延入场、cap 排序裁剪、equal-weight sizing、按 adjusted close 触发 stop-loss、next valid open 平仓、holding 到期平仓，以及无 rebalance / 无 re-entry / 无现金再投入的组合路径。
- 新增 `20260417_0019` migration 与 BacktestRun execution 字段；序列化层同步暴露 `ranking_policy_id`、`excluded_securities`、`portfolio_value`、`position_count_after_exclusions`、`cumulative_return`、`equity_curve`、`per_security_returns`。
- 本轮 review 修复了两个 provenance 缺口：空组合与 `failed-data-insufficient` run 现在都会持久化 dataset span/checksum；当未来数据不足以覆盖完整 entry deferral window 时，run 会返回 `failed-data-insufficient`，不再误报成已完成的空组合。

### 文件清单

- apps/api/src/stockanalyse_api/domain/backtests/models.py
- apps/api/src/stockanalyse_api/services/backtesting.py
- apps/api/src/stockanalyse_api/services/portfolio_backtest.py
- apps/api/src/stockanalyse_api/services/portfolio_backtest_defaults.py
- apps/api/migrations/versions/20260417_0019_add_portfolio_return_execution_fields.py
- apps/api/tests/test_backtesting.py

### 变更日志

- 2026-04-17: Story 5.2 portfolio-return 重做版本创建（v3 patch）。执行引擎独立于 condition-hit 旧路径。
- 2026-04-17: 完成 portfolio-return execution engine、execution schema/migration 与后端测试；review 后补齐 failed/empty-run dataset provenance，并把 entry deferral window 数据不足归类为 `failed-data-insufficient`。
