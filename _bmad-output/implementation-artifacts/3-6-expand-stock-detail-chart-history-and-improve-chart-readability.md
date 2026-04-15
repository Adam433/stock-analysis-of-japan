# 故事 3.6: Expand Stock Detail Chart History and Improve Chart Readability

状态: done

## 用户故事

作为用户，  
我希望个股详情图表默认展示足够的历史区间，并且最新区域保持可读，  
以便我把详情页当成可信的复盘界面，而不是先和图表显示问题搏斗。

## 验收标准

1. 假设用户打开个股详情页，当 K 线图渲染时，那么它默认展示足够的历史上下文，而不是只给一个过窄的近期切片。
2. 假设详情图表后续需要继续扩展历史，当实现方调整时，那么它遵守后端支持的边界，不让前端自行推断权威历史窗口。
3. 假设 RPS 面板已渲染，当用户检查最新区域时，那么固定标签或说明不会遮挡最近的重要曲线和值。
4. 假设详情页展示交易日、筛选日或图表相关日期，当用户查看时，那么这些日期使用本地化的“日期-only”格式，适合日线分析。
5. 假设用户在主工作流之间切换，当进入个股详情页时，那么顶部导航仍然保持和其他主页面一致的结构化导航样式。

## 任务 / 子任务

- [x] 扩展 stock detail 后端图表历史窗口。 (AC: 1, 2)
  - [x] 审查 `chart_data.py` 当前 K 线与指标历史窗口的限制。
  - [x] 将默认历史窗口扩展到能支持常规多月/近一年复盘的范围。
  - [x] 保持“历史窗口由后端定义”的边界，不在前端硬编码权威可视范围。
- [x] 提升 `StockDetailCharts` 的最新区域可读性。 (AC: 3)
  - [x] 检查当前 RPS 图例与终点展示是否会压住最新值附近的观察区域。
  - [x] 采用预留图例区、偏移或轻量布局调整，避免最近区域被固定元素遮挡。
  - [x] 继续沿用 `lightweight-charts`，不要回退到手写 SVG 或另起一套图表实现。
- [x] 统一 stock detail 工作流里的日期表现。 (AC: 4)
  - [x] 审查 `StockDetailView.tsx` 与相关页面里所有图表邻近日期文案。
  - [x] 把适合日线分析的日期改成本地化 date-only 格式，不显示时分秒。
- [x] 验证顶部导航与详情页壳层一致性。 (AC: 5)
  - [x] 对照首页、策略配置页、观察列表页、回测页的 `top-nav` 结构。
  - [x] 避免把导航退化成描述性段落或与主页面不同的结构。
- [x] 补测试与验证。 (AC: 1, 2, 3, 4, 5)
  - [x] 更新 `test_chart_data.py`，确保扩展后的历史窗口与对齐关系仍成立。
  - [x] 运行受影响前端 lint。
  - [x] 记录需要人工确认的图表可读性点，尤其是最新区域与日期显示。

## 完成说明

- 将 `chart_data.py` 的个股详情默认 K 线窗口扩展到 `250`，并保持 `indicator_history` 继续与该后端定义窗口对齐。
- 在 `StockDetailCharts.tsx` 为时间轴增加右侧留白，并关闭 RPS 线末值固定标签，减轻最近区域的视觉挤压。
- 在 `StockDetailView.tsx` 增加图表上下文卡片，展示本地化的筛选交易日和默认历史范围；日期采用不受时区回拨影响的 date-only 格式。
- 保持 stock detail 页面 `top-nav` 结构与其他主工作流一致，没有引入新的导航样式分叉。

## 验证记录

- `/Users/adam/Code/stockAnalyse/.venv/bin/python -m unittest apps.api.tests.test_chart_data apps.api.tests.test_screening apps.api.tests.test_backtesting`
- `npm --prefix apps/web run lint`
- 第一轮 review 发现并修复 `YYYY-MM-DD` 直接 `new Date()` 引发的时区回拨问题，第二轮 review 无新增发现。

## 开发备注

- 当前后端 `get_stock_detail_payload()` 只抓取最近 `120` 根 K 线，已经明显偏窄，是本故事最直接的实现入口。 [Source: apps/api/src/stockanalyse_api/services/chart_data.py]
- `indicator_history` 当前起点绑定到 `candle_rows[0].trade_date`，因此只要扩大后端 K 线窗口，RPS 历史窗口也会一起扩展；不要让前端自行拼接更多历史。 [Source: apps/api/src/stockanalyse_api/services/chart_data.py]
- `StockDetailCharts.tsx` 当前调用 `fitContent()` 并把图例放在图表下方，语义边界已经比较清楚，但还没有显式处理“默认显示多少历史”与“最新区域读图空间”两个问题。 [Source: apps/web/src/components/stocks/StockDetailCharts.tsx]
- `StockDetailView.tsx` 目前把筛选任务时间显示为带时分的 timestamp，这对“任务执行时间”可以接受，但图表邻近/交易日语境的日期应收敛到本地化 date-only 表现。 [Source: apps/web/src/components/stocks/StockDetailView.tsx]
- UX 增量文档已经明确三件事：默认应加载更长历史、RPS 固定标签不能遮住最新值区域、详情页日期应使用本地化 date-only。 [Source: _bmad-output/planning-artifacts/ux-followups-2026-04-15.md]
- 架构增量文档要求历史窗口由后端定义；若未来需要按左侧边界增量加载，也必须以“后端支持的 contract”为前提。这个故事不需要发明前端自决的历史加载协议。 [Source: _bmad-output/planning-artifacts/architecture.md]
- `3.5` 已经把“正式筛选信号”和“仅解释用途”分层完成，本故事应在那个基础上继续改善可读性，而不是重写详情页语义结构。 [Source: _bmad-output/implementation-artifacts/3-5-clarify-rps-chart-semantics-and-explainability-boundaries.md]

## 实施建议

- 优先走“扩大后端默认窗口 + 前端保持轻量展示调整”的最短路径。
- 如果 `fitContent()` 导致默认视野仍不理想，可以在图表创建后施加更明确的可见范围策略，但不要破坏价格图和 RPS 图的联动。
- 日期格式建议抽成小型 helper，避免详情页不同区域再次出现一种是 timestamp、一种是裸 ISO 日期的混搭。
- 仅在必要时修改全局样式；优先复用现有 `chart-panel`、`top-nav`、`status-copy` 等样式结构。

## 测试建议

- 后端至少补一条断言，证明默认返回的 `candlesticks` 长度比旧窗口更长，并且末尾交易日仍与 `screen_run.trade_date` 对齐。
- 保持 `indicator_history` 的最后一项继续与 `latest_indicator_snapshot` 对齐，防止扩大窗口时打破同一权威来源。
- 前端 lint 必须通过。
- 人工验证至少覆盖：
  - 个股详情首次打开时能看到更长的价格历史
  - RPS 图最新区域不被固定说明遮住
  - 图表邻近日期使用本地化 date-only
  - 顶部导航结构与其他主页面一致

## References

- Epic story definition: [epics.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/epics.md:623)
- Previous story context: [3-5-clarify-rps-chart-semantics-and-explainability-boundaries.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/implementation-artifacts/3-5-clarify-rps-chart-semantics-and-explainability-boundaries.md)
- Chart payload service: [chart_data.py](/Users/adam/Documents/GitHub/stockAnalyse/apps/api/src/stockanalyse_api/services/chart_data.py:1)
- Stock detail view: [StockDetailView.tsx](/Users/adam/Documents/GitHub/stockAnalyse/apps/web/src/components/stocks/StockDetailView.tsx:1)
- Stock detail charts: [StockDetailCharts.tsx](/Users/adam/Documents/GitHub/stockAnalyse/apps/web/src/components/stocks/StockDetailCharts.tsx:1)
- Stock detail page shell: [page.tsx](/Users/adam/Documents/GitHub/stockAnalyse/apps/web/src/app/stocks/[instrumentId]/page.tsx:1)
- UX increment: [ux-followups-2026-04-15.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/ux-followups-2026-04-15.md:10)
- Architecture increment: [architecture.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/architecture.md:884)
