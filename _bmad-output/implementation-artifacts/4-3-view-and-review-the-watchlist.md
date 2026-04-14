# 故事 4.3: View and Review the Watchlist

状态: done

## 用户故事

作为用户，  
我希望revisit my watchlist entries with their saved context，  
以便my daily research workflow continues across sessions。

## 验收标准

1. 假设one or more watchlist entries exist，当the user opens the watchlist view，那么the system displays the stored watchlist securities。
2. 假设a watchlist entry is displayed，当the user reviews it，那么the saved note, observation reason, and added date are visible。

## 任务 / 子任务

- [x] Add a dedicated watchlist review page. (AC: 1, 2)
  - [x] Add a `/watchlist` route in the web app.
  - [x] Load persisted watchlist entries from the existing watchlist API.
- [x] Render saved watchlist context in a reviewable layout. (AC: 1, 2)
  - [x] Show stored securities with symbol, exchange, name, added date, observation reason, and note.
  - [x] Keep watchlist editing/removal available directly from the review surface.
- [x] Connect the watchlist page into the main workflow. (AC: 1)
  - [x] Add navigation links from the existing health, screen, and stock-detail routes.
- [x] Validate the frontend implementation. (AC: 1, 2)
  - [x] Run frontend lint.
  - [x] Run frontend build.

## 开发备注

- Story 4.3 should consume the persisted watchlist payload established in Stories 4.1 and 4.2 instead of inventing a separate review-only backend contract. [Source: _bmad-output/implementation-artifacts/4-1-add-and-remove-qualified-stocks-from-the-watchlist.md, _bmad-output/implementation-artifacts/4-2-persist-watchlist-notes-observation-reasons-and-added-dates.md]
- The watchlist workflow is supposed to preserve continuity across sessions, so the review view should emphasize saved context rather than acting like a blank list of tickers. [Source: _bmad-output/planning-artifacts/prd.md:151,255,282]
- Epic 4 closes once the user can add candidates, preserve research context, and later revisit the list in a dedicated view. [Source: _bmad-output/planning-artifacts/epics.md:438-490]

## 开发代理记录

### 使用的代理模型

GPT-5.4

### 调试日志参考

- Added a dedicated `/watchlist` page and a watchlist review panel.
- Reused persisted watchlist payloads to show saved note, observation reason, and added date.
- Connected the page into the main navigation from the existing app routes.
- Verified with `npm run lint` and `npm run build`.

### 完成说明

- The web app now has a dedicated watchlist review view showing saved symbols and their research context.
- Watchlist entries can still be edited or removed directly from that review surface.
- Epic 4 now has an end-to-end watchlist workflow from candidate capture to later review.

### 文件清单

- _bmad-output/implementation-artifacts/4-3-view-and-review-the-watchlist.md
- apps/web/src/app/globals.css
- apps/web/src/app/page.tsx
- apps/web/src/app/screen/page.tsx
- apps/web/src/app/stocks/[instrumentId]/page.tsx
- apps/web/src/app/watchlist/page.tsx
- apps/web/src/components/watchlist/WatchlistReviewPanel.tsx
- apps/web/src/components/watchlist/WatchlistToggleButton.tsx

### 变更日志

- 2026-04-14: Added a dedicated watchlist review page and connected it into the main workflow.
