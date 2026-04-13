# Story 4.3: View and Review the Watchlist

Status: done

## Story

As a user,  
I want to revisit my watchlist entries with their saved context,  
so that my daily research workflow continues across sessions.

## Acceptance Criteria

1. Given one or more watchlist entries exist, when the user opens the watchlist view, then the system displays the stored watchlist securities.
2. Given a watchlist entry is displayed, when the user reviews it, then the saved note, observation reason, and added date are visible.

## Tasks / Subtasks

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

## Dev Notes

- Story 4.3 should consume the persisted watchlist payload established in Stories 4.1 and 4.2 instead of inventing a separate review-only backend contract. [Source: _bmad-output/implementation-artifacts/4-1-add-and-remove-qualified-stocks-from-the-watchlist.md, _bmad-output/implementation-artifacts/4-2-persist-watchlist-notes-observation-reasons-and-added-dates.md]
- The watchlist workflow is supposed to preserve continuity across sessions, so the review view should emphasize saved context rather than acting like a blank list of tickers. [Source: _bmad-output/planning-artifacts/prd.md:151,255,282]
- Epic 4 closes once the user can add candidates, preserve research context, and later revisit the list in a dedicated view. [Source: _bmad-output/planning-artifacts/epics.md:438-490]

## Dev Agent Record

### Agent Model Used

GPT-5.4

### Debug Log References

- Added a dedicated `/watchlist` page and a watchlist review panel.
- Reused persisted watchlist payloads to show saved note, observation reason, and added date.
- Connected the page into the main navigation from the existing app routes.
- Verified with `npm run lint` and `npm run build`.

### Completion Notes List

- The web app now has a dedicated watchlist review view showing saved symbols and their research context.
- Watchlist entries can still be edited or removed directly from that review surface.
- Epic 4 now has an end-to-end watchlist workflow from candidate capture to later review.

### File List

- _bmad-output/implementation-artifacts/4-3-view-and-review-the-watchlist.md
- apps/web/src/app/globals.css
- apps/web/src/app/page.tsx
- apps/web/src/app/screen/page.tsx
- apps/web/src/app/stocks/[instrumentId]/page.tsx
- apps/web/src/app/watchlist/page.tsx
- apps/web/src/components/watchlist/WatchlistReviewPanel.tsx
- apps/web/src/components/watchlist/WatchlistToggleButton.tsx

### Change Log

- 2026-04-14: Added a dedicated watchlist review page and connected it into the main workflow.
