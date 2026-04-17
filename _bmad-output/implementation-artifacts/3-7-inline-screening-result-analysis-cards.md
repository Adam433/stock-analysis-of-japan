# 故事 3.7: Inline Screening Result Analysis Cards

状态: ready-for-dev

> **v3 增量补丁（2026-04-17）新增 story**：筛选结果区（`/screen` 页面的"最近一次筛选结果"区域）当前只显示入选股票的文本信息（symbol、RPS、距高点等）。用户在 2026-04-16 页面验收中要求：**在结果卡片内直接展示 K 线与财务概览**，而不必逐只点进个股详情。本 story 在复用 `lightweight-charts` 与现有 stock-detail 数据契约的前提下，为每张结果卡片引入"内联分析区"。滚动增量加载是 Story 3.8 的职责，本 story 只负责单卡片的分析内容与后端契约。

## 用户故事

作为用户，
我希望每张筛选结果卡片在原地就展示该股票的 1 年 K 线、估值指标（PE、PB）和最近 5 个财年的净利润柱状图，
以便我在不进入个股详情页的情况下，就能横向比较候选股的价格走势、估值与盈利情况。

## 验收标准

1. **AC1（1 年 K 线内联）**：给定一个已完成的 screen run 与其 `qualified_results`，当用户在 `/screen` 页面查看任意一张结果卡片时，该卡片必须在卡内渲染该证券的 1 年（约 252 trading days）K 线图。图表必须由 `lightweight-charts` 的 `CandlestickSeries` 渲染，**禁止**手写 SVG 或引入新的图表库（FR67）。

2. **AC2（IPO / 历史不足场景）**：给定一只证券的可用历史不足 1 年（例如近期 IPO），当其内联 K 线渲染时：
   - 只绘制可用部分，不得对较短序列做时间轴拉伸、前向填充或回填虚拟数据；
   - 卡片上必须**显式显示**一个文案，提示当前展示窗口短于 1 年（例如 `历史数据仅覆盖 XX 个交易日`），而不是静默缩短。

3. **AC3（5 财年净利润 + 同轴 PE/PB）**：每张结果卡片必须渲染一张覆盖该公司最近**最多 5 个财年**的净利润柱状图，并且：
   - 每根柱子旁边或同一时间轴上必须同时呈现**该财年**的 PE 与 PB 值（不是当前 trailing PE/PB）；
   - 净利润、PE、PB 必须共享同一时间轴（按该公司自己的财年排序），使用户可以把"盈利能力"与"估值"对齐阅读；
   - 实现上优先复用 `lightweight-charts` 的 `HistogramSeries`（净利润）+ 数值标注（PE/PB）。**禁止**为此新引入第二套图表框架。

4. **AC4（净亏损年度的 PE 约定）**：给定某个财年净利润为负（净亏损），当该财年的 PE 需要展示时，卡片必须采用**单一显式约定**展示 PE：
   - 要么显示负 PE 值并在 UI 文案中解释（例如 `PE: -12.3（净亏损）`）；
   - 要么显示 `N/A` 并在 UI 文案中说明"净亏损，PE 不适用"；
   - 实现必须在整套 UI 中选择**其中一种**且全局一致。**禁止**静默省略 PE 标签、用正值替代、或跨卡片混用两种约定。约定选择必须在代码注释与 Dev Notes 的"Fiscal-Year Valuation Convention"小节锁定。

5. **AC5（公司自有财年标签）**：给定多只卡片展示的证券具有不同的财年结束月（例如 3 月 vs 12 月财年），当财年柱子渲染时，**每张卡片**必须按**该公司自己报告的财年**排序与标注（不是把它们归一到日历年）；标签必须让财年结束月可见（例如 `FY2024（03 月结束）`），避免跨卡片误读。

6. **AC6（缺失数据显式化）**：给定某个财年的 PE、PB 或净利润缺失，当卡片渲染时，必须用**显式**方式呈现缺失（例如一根带有 `数据缺失` 标签的灰色占位柱，或 `—` 替代数值），**禁止**：
   - 插值或外推未知值；
   - 静默省略该财年；
   - 用相邻年份替代。

7. **AC7（后端契约扩展 + 复用）**：内联分析数据的后端契约必须通过**扩展**现有 stock-detail 数据契约来提供，而不是开第二套 charting 协议：
   - 新增端点 `GET /stocks/{instrument_id}/inline-analysis?screen_run_id=...` 返回**精简版**载荷，复用 `chart_data.py` 已有的 candlestick 组装逻辑（约 1 年窗口 ≈ 252 行）；
   - 该端点**不**返回 `indicator_history`、`rule_breakdown`、`latest_indicator_snapshot`（这些属于 stock detail 页面的重载荷，内联卡片不需要）；
   - 端点**新增**返回字段：`valuation_by_fiscal_year: list[{fiscal_year_label, fiscal_year_end_month, net_income, net_income_currency, pe, pb, data_status}]`，最多 5 项，按财年时间升序排列；
   - 当历史不足 1 年时，candlesticks 的长度如实反映可用历史，并在 payload 顶层返回 `candlestick_window_days_available`（整数，可能 < 252）；
   - 当 fundamentals 数据缺失时，`data_status` 字段必须显式标注为 `"missing"`，不得静默返回 null 与无说明。

8. **AC8（Fundamentals 领域 + 持久化）**：新增 fundamentals domain 以承载财年级财务与估值数据：
   - 新增 `apps/api/src/stockanalyse_api/domain/fundamentals/models.py` 定义 `FundamentalsAnnual`（instrument_id, fiscal_year_end_date, fiscal_year_label, net_income, net_income_currency, pe, pb, source, source_as_of_date, data_status）；
   - `data_status` 与 `market_data_daily.data_status` 共用同一语义约定（`complete` / `partial` / `missing`），禁止新造一套 status 词汇；
   - 新增 Alembic migration `20260417_0019_add_fundamentals_annual.py`（在 Stories 5.6/5.1 v3 新增 migration `20260417_0017/_0018` 之后继续编号）；
   - `source` 字段用于记录数据来源 provider name，沿用现有 `credential_boundary=backend_only` 的 provider 契约。

9. **AC9（Fundamentals provider + 刷新路径）**：为 fundamentals 提供一条最小可行的数据供给路径：
   - 新增 `apps/api/src/stockanalyse_api/services/ingestion/providers/yahoo_finance_fundamentals_provider.py`，**沿用**现有 `yahoo_finance_chart_provider.py` 的 urllib + certifi 请求模式与 `credential_boundary=backend_only` 约束，**禁止**引入 `yfinance` 等新依赖（保持 `apps/api/pyproject.toml` 依赖集不扩大）；
   - provider 只拉取最近 5 个财年的净利润 + 当期 PE/PB 注记，返回 `list[ProviderFundamentalsAnnual]`（定义在 `provider_models.py`）；
   - refresh 路径不要求加入 daily 自动 refresh runtime（那是 Epic 1 边界）；本 story 只要求一条 `scripts/refresh_fundamentals.py` 或等价的 `services/fundamentals_refresh.py:refresh_instrument_fundamentals(session, instrument_id)` 函数，在 stock-detail 或 inline-analysis 读取时**按需**触发（lazy refresh），并缓存到 `fundamentals_annual`；
   - 当 provider 返回失败或限流时，**不**删除已有缓存行；`data_status` 仍保留既存值（例如 `complete` 或 `partial`），只更新 `source_as_of_date`。

10. **AC10（UI 卡片组件）**：新增 `apps/web/src/components/screen/ResultAnalysisCard.tsx`：
    - 接受 `instrumentId` / `symbol` / `screenRunId` / `analysisPayload | null | "loading" | "failed"` 等 props；
    - 内部渲染两块 `lightweight-charts` 图表（1 年 K 线 + 财年净利润柱）与 PE/PB 文本摘要；
    - 当 `analysisPayload === "failed"` 时暴露一个**显式**的"重试加载"按钮（与 AC9 的失败语义对齐），并显示具体错误文案；
    - **本 story 不实现懒加载 / IntersectionObserver**——`StrategyConfigPanel.tsx` 在本 story 内**同步**为每张卡片请求内联分析（一次性全部加载）。懒加载与分批是 Story 3.8 的职责，本 story 故意保持同步加载以简化语义。

11. **AC11（Trust & 可访问性承继）**：内联分析卡片必须沿用 Epic 6 既有的可访问性与不可信颜色约束：
    - 净利润正 / 负颜色差异**不得**是唯一表意通道（必须同时有文字标签 `盈利` / `亏损` 或正负号）；
    - 财年标签、PE、PB 必须以**文本**形式可读出（屏幕阅读器友好），不仅作为图表悬浮；
    - 日期格式沿用 `apps/web/src/lib/formatters.ts` 中已有的 date-only 本地化 helper（来自 Story 3.6），不要再造一套。

## 任务 / 子任务

- [ ] Fundamentals 领域 + migration（AC: 8）
  - [ ] 创建 `apps/api/src/stockanalyse_api/domain/fundamentals/__init__.py` 与 `models.py`，定义 `FundamentalsAnnual` 与模块级常量 `FUNDAMENTALS_DATA_STATUS_VALUES = ("complete", "partial", "missing")`（复用 market_data 的 status 语义）
  - [ ] 创建 migration `apps/api/migrations/versions/20260417_0019_add_fundamentals_annual.py`，`down_revision = "20260417_0018"`（承接 Story 5.1 v3 migration 后的空闲编号），`batch_alter_table` 风格建表；index on `(instrument_id, fiscal_year_end_date)` unique
  - [ ] 在 `apps/api/src/stockanalyse_api/domain/instruments/models.py` 的 `Instrument` 上加 `fundamentals_annual = relationship("FundamentalsAnnual", back_populates="instrument")` 以保持 ORM 对称（参考已有 `daily_market_data` 关系）
- [ ] Fundamentals provider + lazy refresh 服务（AC: 9）
  - [ ] 新建 `apps/api/src/stockanalyse_api/services/ingestion/provider_models.py` 中 `ProviderFundamentalsAnnual` 数据类（字段同 AC8 模型的子集 + source / source_as_of_date）
  - [ ] 新建 `apps/api/src/stockanalyse_api/services/ingestion/providers/yahoo_finance_fundamentals_provider.py`，`provider_name = "yahoo_finance_fundamentals"`，复用 `yahoo_finance_chart_provider.py` 的 urllib + certifi SSL context 模式（不要另建一套 HTTP 客户端）；目标 endpoint 建议 `https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}?modules=incomeStatementHistory,summaryDetail,defaultKeyStatistics`
  - [ ] 在 `services/ingestion/providers/registry.py` 注册该 provider
  - [ ] 新建 `apps/api/src/stockanalyse_api/services/fundamentals_refresh.py`，暴露 `refresh_instrument_fundamentals(session, *, instrument_id: int, provider=None) -> None`：upsert 近 5 个财年；失败时保留既存 `data_status`，仅更新 `source_as_of_date`
  - [ ] **不**修改 `services/ingestion/refresh_service.py`（daily universe refresh runtime）；fundamentals 走"按需 lazy refresh"路径
- [ ] Inline analysis 端点（AC: 7）
  - [ ] 新建 `apps/api/src/stockanalyse_api/services/inline_analysis.py::get_inline_analysis_payload(session, *, instrument_id, screen_run_id, candle_window_days=252, fiscal_year_limit=5)`，复用 `chart_data.py` 中 candlestick 组装逻辑（抽取成公共 helper `_collect_candlesticks(session, instrument_id, trade_date_cutoff, limit)` 若两处都能复用则放到 `chart_data.py` 顶层）
  - [ ] payload 顶层字段：`instrument`（id/symbol/exchange/name/currency）、`screen_run_ref`（id + trade_date，用于防错判定）、`candlesticks`、`candlestick_window_days_available`、`valuation_by_fiscal_year`、`generated_at`
  - [ ] 新建路由 `apps/api/src/stockanalyse_api/api/routes/stocks.py` 中 `@router.get("/{instrument_id}/inline-analysis")`，接受 `screen_run_id: int | None = None`；404 when instrument or screen run not found
  - [ ] 读取时若 `fundamentals_annual` 缺数据或 source_as_of_date > 7 天，调用 `refresh_instrument_fundamentals(...)` 做一次 lazy refresh（容错：失败时返回 `data_status="missing"` 的历史记录而不 500）
- [ ] Frontend 数据契约 + 页面（AC: 10）
  - [ ] 在 `apps/web/src/lib/types.ts` 追加 `InlineAnalysisPayload` 与 `FiscalYearValuation` 类型
  - [ ] 在 `apps/web/src/lib/apiPaths.ts` 追加 `stockInlineAnalysis(instrumentId, screenRunId?)`
  - [ ] 新建 `apps/web/src/components/screen/ResultAnalysisCard.tsx`，内部用两份独立 `createChart()` 实例（价格 / 财年），使用 `HistogramSeries` + 文本 label 表达净利润与 PE/PB
  - [ ] 在 `apps/web/src/components/screen/StrategyConfigPanel.tsx` 的 `result-card` 渲染块（当前第 402-466 行）中嵌入 `<ResultAnalysisCard>`，**同步**为每条结果请求 `/stocks/{id}/inline-analysis?screen_run_id=...`（懒加载留给 Story 3.8）
  - [ ] 缺失 / 失败文案沿用既有 `status-copy` 与 `workflow-trust-banner` 样式，**不要**新增一套 UI primitive
- [ ] Fiscal-Year Valuation Convention（AC: 4, 5）
  - [ ] 在 `ResultAnalysisCard.tsx` 顶部注释中锁定 "PE 约定"：**净亏损年度 → 显示 `N/A` + tooltip `净亏损，PE 不适用`**（选择 `N/A` 方案而非负值方案，理由：负 PE 跨股票比较会误导用户）
  - [ ] 财年标签格式统一为 `FY{YYYY}（{MM} 月结束）`，例如 `FY2024（03 月结束）`；该 helper 放到 `apps/web/src/lib/formatters.ts`
- [ ] 可访问性（AC: 11）
  - [ ] 净利润正负额外用符号 / 文字表达（`+` 与 `盈利` / `-` 与 `亏损`）
  - [ ] PE / PB / 净利润数值提供 `aria-label` 或 `dt/dd` 结构，不仅依赖图表 tooltip
- [ ] 测试（AC: 1-11）
  - [ ] `apps/api/tests/test_inline_analysis.py`（新建）：
    - 1 年 candlestick 窗口按 trade_date_cutoff 正确截取；
    - IPO 场景（只有 30 天数据）时 `candlestick_window_days_available = 30`；
    - fundamentals 缺失时 `data_status = "missing"` 且端点不 500；
    - provider 失败时沿用既存缓存行的 `data_status`；
    - fiscal_year_limit 默认 5 且按时间升序；
  - [ ] `apps/api/tests/test_fundamentals_refresh.py`（新建）：upsert / 失败保留已缓存 `data_status` / 近 5 财年截断
  - [ ] `apps/web/tests/components/ResultAnalysisCard.test.tsx`（新建）：
    - loading / loaded / failed 三态渲染；
    - 净亏损年度显示 `N/A`（PE 约定锁定）；
    - 历史不足 1 年时显示短窗口提示；
    - 缺失财年显示 `数据缺失` 占位；
    - 失败态暴露"重试加载"按钮；
  - [ ] 命令：`cd apps/api && PYTHONPATH=src python3 -m unittest tests.test_inline_analysis tests.test_fundamentals_refresh tests.test_chart_data`、`PYTHONPATH=src python3 -m alembic -c alembic.ini upgrade head`、`cd apps/web && npm run lint && npm run build && npm run test`

## 开发备注

- **核心反模式**：用户在 v3 补丁中**明确反对"造轮子"**（Sprint Change Proposal `4.5` 与 `约束原则`）。本 story 的实现必须：
  - 复用 `lightweight-charts`（已是 dependency `^5.1.0`）；**禁止**引入 `chart.js` / `recharts` / `d3` 等第二套图表库；
  - 复用 stock-detail 数据契约的 candlestick 组装逻辑（`chart_data.py`）；**禁止**让前端自行聚合 K 线；
  - 复用现有 `yahoo_finance_chart_provider.py` 的 HTTP 客户端模式；**禁止**新依赖 `yfinance` / `requests` / `httpx`；
  - 复用现有 `data_status = complete/partial/missing` 词汇；**禁止**为 fundamentals 另创 status 枚举。
- **端点拆分理由**：inline-analysis 不复用 `/stocks/{id}/detail` 是因为后者的 payload 包含 `rule_breakdown`、`indicator_history`、`latest_indicator_snapshot`（stock detail 页面重依赖），在结果区批量请求会成倍放大响应体；FR67 只要求"复用数据契约"，不要求共用同一个端点，所以 `chart_data.py` 的组装 helper 可以复用但端点可以分开。
- **Fundamentals provider 选型**：Yahoo Finance `quoteSummary` 提供 `incomeStatementHistory`（近 4 财年年度净利润）、`summaryDetail`（trailing PE）、`defaultKeyStatistics`（priceToBook）。如果只能拿到 4 财年，就返回 4 条（不要伪造第 5 条）。provider 失败时**保留既存缓存**是关键——不能因单次网络抖动回退到 `missing` 覆盖已知数据。
- **净亏损 PE 约定**锁定为 `N/A`（理由：负 PE 值在横向比较时严重误导，例如 `-2` 与 `-80` 的高低完全不代表估值高低；显示 `N/A` 加文字 `净亏损，PE 不适用` 更诚实）。实现时只需在 `valuation_by_fiscal_year` 中给净亏损年度的 `pe` 返回 `null` + 在前端渲染层把 `null` 且 `net_income < 0` 的组合翻译成 `N/A`。
- **不要**尝试在本 story 中"顺便"做 Story 3.8 的滚动增量加载；结果卡片按 v3 patch 分工为两个 story，**同步加载**的实现刚好是 3.8 的反例对照，后续 3.8 实现时只需替换加载调度，不必重写 `ResultAnalysisCard` 内部结构。
- **Migration 编号**沿用现有 `YYYYMMDD_NNNN` 格式，紧随 Story 5.6 anchor (`20260417_0017`) 与 Story 5.1 v3 portfolio launch fields (`20260417_0018`) 之后为 `20260417_0019`。若实际实现时 5.6 / 5.1 v3 编号有调整，本 story 编号跟随 head revision 向后平移，保持单调递增。
- 现有卡片 UI `apps/web/src/components/screen/StrategyConfigPanel.tsx:403` 已经是 `<article className="result-card">` 结构，新增内联分析区应作为该 article 的**内部区块**（例如在 `signal-list` 之后追加 `<ResultAnalysisCard>`），不要把它抬到卡片外或破坏现有 `result-card__title` / `result-summary-grid` 结构。
- 本 story **不**修改 stock detail 页面 (`apps/web/src/app/stocks/[instrumentId]/page.tsx`)；stock detail 仍然使用原 `/stocks/{id}/detail` 端点 + 重载荷，内联分析端点只服务结果区。
- 本 story **不**改变 `/screen/runs/latest` 的现有契约——结果列表还是由那个端点返回；内联分析是 **per-instrument** 的二次请求（为 Story 3.8 的按需加载留接口）。

### Fiscal-Year Valuation Convention（本 story 锁定的产品约定）

- 净亏损年度 PE → **显示 `N/A`**，tooltip `净亏损，PE 不适用`
- 缺失的 PE / PB / 净利润 → **显示 `数据缺失`**（灰色占位柱 + 文本标签），不要与 "N/A" 混用
- 财年标签 → **`FY{YYYY}（{MM} 月结束）`**（按公司自己报告的财年结束月）
- PE / PB 位数 → PE 保留 1 位小数，PB 保留 2 位小数；净利润按货币原位精度 + 千分位
- 正 / 负净利润的颜色 → 绿色 / 红色，但**必须**同时有文字 `盈利` / `亏损` 冗余表达

### Project Structure Notes

- 后端模型：`apps/api/src/stockanalyse_api/domain/fundamentals/models.py`（新建）
- 后端 migration：`apps/api/migrations/versions/20260417_0019_add_fundamentals_annual.py`（新建）
- 后端 provider：`apps/api/src/stockanalyse_api/services/ingestion/providers/yahoo_finance_fundamentals_provider.py`（新建）
- 后端 provider 数据类：`apps/api/src/stockanalyse_api/services/ingestion/provider_models.py`（现有文件追加类）
- 后端 lazy refresh 服务：`apps/api/src/stockanalyse_api/services/fundamentals_refresh.py`（新建）
- 后端 inline-analysis 服务：`apps/api/src/stockanalyse_api/services/inline_analysis.py`（新建）
- 后端路由：`apps/api/src/stockanalyse_api/api/routes/stocks.py`（现有文件追加 endpoint）
- 后端测试：`apps/api/tests/test_inline_analysis.py`、`apps/api/tests/test_fundamentals_refresh.py`（新建）
- 前端类型：`apps/web/src/lib/types.ts`（追加 `InlineAnalysisPayload`、`FiscalYearValuation`）
- 前端 API paths：`apps/web/src/lib/apiPaths.ts`（追加 `stockInlineAnalysis`）
- 前端卡片组件：`apps/web/src/components/screen/ResultAnalysisCard.tsx`（新建）
- 前端宿主修改：`apps/web/src/components/screen/StrategyConfigPanel.tsx:402-466`（在 result-card 内部嵌入 `<ResultAnalysisCard>`）
- 前端格式化 helper：`apps/web/src/lib/formatters.ts`（追加 `formatFiscalYearLabel`）
- 前端测试：`apps/web/tests/components/ResultAnalysisCard.test.tsx`（新建）

### References

- Story 原文与 BDD 验收：[Source: _bmad-output/planning-artifacts/epics.md:722-761]（Story 3.7 的 7 条 Given/When/Then）
- FR65 / FR66 / FR67 / NFR25 原文：[Source: _bmad-output/planning-artifacts/prd.md:418-420,486]
- v3 patch 变更来源：[Source: _bmad-output/planning-artifacts/sprint-change-proposal-2026-04-16-page-review-followups.md:47-53,80-82,167-182]
- 复用的 chart 组装逻辑：[Source: apps/api/src/stockanalyse_api/services/chart_data.py:51-213]
- 现有 Yahoo provider（新 fundamentals provider 的样板）：[Source: apps/api/src/stockanalyse_api/services/ingestion/providers/yahoo_finance_chart_provider.py:1-40]
- 现有 Provider 注册表：[Source: apps/api/src/stockanalyse_api/services/ingestion/providers/registry.py]
- 现有 stock detail 路由：[Source: apps/api/src/stockanalyse_api/api/routes/stocks.py:1-23]
- 现有结果卡片 UI：[Source: apps/web/src/components/screen/StrategyConfigPanel.tsx:402-466]
- Chart 库 / 日期 helper 基线：[Source: apps/web/src/components/stocks/StockDetailCharts.tsx:1-80]（HistogramSeries 与 CandlestickSeries 的 lightweight-charts 用法）
- 前一故事语境（Epic 3 收尾基线）：[Source: _bmad-output/implementation-artifacts/3-6-expand-stock-detail-chart-history-and-improve-chart-readability.md]
- Epic 3 retrospective：[Source: _bmad-output/implementation-artifacts/epic-3-retro-2026-04-16.md]
- 架构对 chart 数据 / 增量加载的既有约束：[Source: _bmad-output/planning-artifacts/architecture.md:883-888]（后端定义历史窗口；前端不推断）
- 现有 `data_status` 语义（complete/partial/missing）来源：[Source: apps/api/src/stockanalyse_api/domain/market_data/models.py]

## 开发代理记录

### 使用的代理模型

{{agent_model_name_version}}

### 调试日志参考

### 完成说明

### 文件清单

### 变更日志

- 2026-04-17: Story 3.7 创建（v3 增量补丁）。作为 Epic 3 在 v3 阶段的重开第一条 story，引入 fundamentals domain + inline-analysis 端点 + `ResultAnalysisCard` 组件。懒加载由 Story 3.8 承接。
