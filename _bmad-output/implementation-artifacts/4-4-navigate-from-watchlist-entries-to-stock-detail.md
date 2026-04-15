# 故事 4.4: Navigate from Watchlist Entries to Stock Detail

状态: done

## 用户故事

作为用户，  
我希望每个观察列表条目都能直接进入对应的个股详情页，  
以便我继续研究时不需要回到筛选页重新找代码和上下文。

## 验收标准

1. 假设观察列表里已有条目，当用户点击股票代码或主要详情入口时，那么产品直接打开该标的的个股详情工作流。
2. 假设该条目带有备注和观察原因，当用户从观察列表钻取到详情页时，那么导航保留足够的上下文，让用户知道自己仍在 watchlist 复盘链路里。

## 任务 / 子任务

- [x] 为 watchlist 条目增加直接详情入口。 (AC: 1, 2)
  - [x] 让 symbol 或主操作按钮直接链接到 stock detail。
  - [x] 保持链接可键盘访问，并在视觉上明确它是主入口。
- [x] 让 stock detail 在缺少显式 `screen_run_id` 时仍可打开。 (AC: 1, 2)
  - [x] 为指定 instrument 解析最近可用的筛选上下文。
  - [x] 保持已有“显式 `screen_run_id`”路径不回退。
  - [x] 如果完全找不到任何可用筛选结果，返回清晰错误而不是崩溃。
- [x] 补测试与验证。 (AC: 1, 2)
  - [x] 扩展 `test_chart_data.py` 覆盖“未传 `screen_run_id` 的详情解析”。
  - [x] 运行前端 lint。

## 开发备注

- 当前 watchlist 页面只有“返回筛选”链接，没有 direct detail affordance。 [Source: apps/web/src/components/watchlist/WatchlistReviewPanel.tsx]
- 当前 stock detail 页面强制要求 `screen_run_id` 查询参数；这会让 watchlist 无法自然跳转。 [Source: apps/web/src/app/stocks/[instrumentId]/page.tsx, apps/api/src/stockanalyse_api/api/routes/stocks.py]
- 最稳妥的实现是：显式 `screen_run_id` 仍优先；缺省时，后端为 instrument 解析最近可用的筛选结果上下文。这样 watchlist 无需伪造参数，也不会破坏现有筛选结果跳转。 [Source: apps/api/src/stockanalyse_api/services/chart_data.py]
- UX 文档明确要求每个 watchlist row 提供清晰的详情入口，而且该入口必须键盘可达。 [Source: _bmad-output/planning-artifacts/ux-followups-2026-04-15.md]

## 完成说明

- 在 `WatchlistReviewPanel.tsx` 中把 symbol 和主操作区都接成个股详情入口，用户可直接从 watchlist 进入研究页。
- 在 `chart_data.py` 中增加“最近可用筛选上下文”解析逻辑；显式 `screen_run_id` 仍优先，缺省时回退到该 instrument 最近的可用筛选结果。
- `stocks.py` 和 stock detail 页面同步支持“无 `screen_run_id`”访问路径。
- 扩展 `test_chart_data.py`，覆盖 watchlist 场景下的隐式上下文解析。

## 验证记录

- `/Users/adam/Code/stockAnalyse/.venv/bin/python -m unittest apps.api.tests.test_chart_data apps.api.tests.test_watchlist apps.api.tests.test_screening`
- `npm --prefix apps/web run lint`
- 第一轮 review 无发现，第二轮复审无新增发现。

## References

- Epic story definition: [epics.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/planning-artifacts/epics.md:707)
- Previous story context: [4-3-view-and-review-the-watchlist.md](/Users/adam/Documents/GitHub/stockAnalyse/_bmad-output/implementation-artifacts/4-3-view-and-review-the-watchlist.md)
- Watchlist review panel: [WatchlistReviewPanel.tsx](/Users/adam/Documents/GitHub/stockAnalyse/apps/web/src/components/watchlist/WatchlistReviewPanel.tsx:1)
- Stock detail route: [page.tsx](/Users/adam/Documents/GitHub/stockAnalyse/apps/web/src/app/stocks/[instrumentId]/page.tsx:1)
- Stocks API route: [stocks.py](/Users/adam/Documents/GitHub/stockAnalyse/apps/api/src/stockanalyse_api/api/routes/stocks.py:1)
- Stock detail service: [chart_data.py](/Users/adam/Documents/GitHub/stockAnalyse/apps/api/src/stockanalyse_api/services/chart_data.py:1)
