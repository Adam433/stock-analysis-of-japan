# 故事 3.8: Incremental Result-Panel Data Loading

状态: review

> **v3 增量补丁（2026-04-17）新增 story，依赖 Story 3.7**：Story 3.7 已为每张筛选结果卡片引入内联分析区（`ResultAnalysisCard`）与后端端点 `GET /stocks/{instrument_id}/inline-analysis`，但**同步一次性加载**所有候选的内联分析。当一次筛选命中大量候选时（例如 100 只），首屏会一次性请求 N 份 K 线 + 财年 payload，违反 NFR25（50 只以内 3 秒首屏；50 只以上必须先渲染列表骨架再渐进填充）。本 story 把 `ResultAnalysisCard` 的加载调度从"同步全部"切换为"按视口增量加载"，**不改变**卡片内部渲染、后端契约或 fundamentals domain。

## 用户故事

作为用户，
我希望在命中大量候选时，只有视口内或即将进入视口的结果卡片才请求其内联分析数据，
以便长列表不会因为一次性加载全部 K 线与财年 payload 而拖慢首屏或耗尽后端/网络带宽。

## 验收标准

1. **AC1（≥ 20 触发增量加载）**：给定一次 screen run 产出 **≥ 20** 只 qualified securities，当 `/screen` 页面的结果区首次渲染时，只有**视口内或近视口**的卡片请求自己的 inline-analysis payload；视口下方未进入"预加载区"的卡片必须保持在 `idle` 状态（**不**发起请求）。

2. **AC2（< 20 一次性加载）**：给定一次 screen run 产出 **< 20** 只 qualified securities，当结果区首次渲染时，所有卡片**立即**并发请求自己的 inline-analysis payload（与 Story 3.7 的同步加载行为一致）；增量加载调度器必须**不**参与该路径，避免在小列表场景引入不必要的延迟。

3. **AC3（滚动渐进加载）**：给定 ≥ 20 场景下用户向下滚动结果区，当额外卡片靠近视口（进入 `rootMargin` 预加载缓冲区）时，调度器必须按批次发起后续卡片的 inline-analysis 请求；同一批内并发数必须受 `MAX_CONCURRENT_INLINE_LOADS`（见 Dev Notes）限制，不得一次性全部踢出。

4. **AC4（排序 / 过滤时重置加载状态）**：给定用户在结果列表上应用新的排序或过滤（未来可能加入的结果区排序 UI；当前 `StrategyConfigPanel.tsx` 已具备排序/过滤钩子点），当新顺序生效时，调度器的 `loaded` / `loading` / `failed` per-card 状态必须按"新视口内优先"**重置**，确保此时视口内的卡片是第一批被请求的，而不是沿用旧顺序的首批卡片。

5. **AC5（失败显式化 + 独立）**：给定某张卡片的 inline-analysis 请求失败，当该卡片位于视口内时：
   - 该卡片必须显示**显式错误状态**（文本 + 图标，沿用 Story 3.7 AC10 的"重试加载"按钮）；
   - 该失败**不得**阻塞其它卡片的加载；调度器必须继续处理队列中的其他卡片（无论是相邻的还是已在视口内等待的）；
   - 错误状态必须携带足够信息让用户决定是否重试（HTTP 状态 / 本地网络错误文案）。

6. **AC6（失败卡片不静默重试）**：给定一张 inline-analysis 曾失败的卡片，当用户滚出视口后再滚回该卡片时，调度器**不得**自动重新请求；卡片必须保持 `failed` 状态等待用户点击"重试加载"按钮。只有用户显式点击重试才切回 `loading → loaded | failed`；这是防止长列表中一只"总是失败"的卡片在用户反复滚动时静默消耗后端带宽。

7. **AC7（NFR25 性能约束）**：
   - 对 ≤ 50 只候选的 screen run，首批内联卡片（视口内可见的卡片）必须在 **3 秒内**完成首屏渲染（payload 加载完成并显示图表）；
   - 对 > 50 只候选的 screen run，结果列表**骨架**（卡片壳 + 股票文本信息 + 分析区 skeleton 占位）必须在 **3 秒内**渲染；分析区的 payload 随滚动渐进填充；
   - 骨架态（`idle` 状态）必须在代码中显式用 skeleton 占位符（不是 `null`），保持卡片高度稳定避免滚动跳动；
   - 测试时可用 Chromium DevTools Network throttling 或 MSW / vitest fake timers 来模拟慢响应并验证骨架先行。

8. **AC8（不回归 Story 3.7 契约）**：本 story **不得**修改：
   - `ResultAnalysisCard` 的内部渲染逻辑与 props 形态（除接收 `analysisPayload | "loading" | "failed" | "idle"` 的 discriminated union 已在 3.7 内暴露外，不加新 prop）；
   - 后端 `GET /stocks/{instrument_id}/inline-analysis` 端点的请求 / 响应 schema；
   - fundamentals domain / migration / provider / 刷新路径。
   测试必须显式断言 Story 3.7 的现有测试（`ResultAnalysisCard.test.tsx`、`test_inline_analysis.py`）在本 story 完成后**全部保持绿色**。

## 任务 / 子任务

- [x] 调度器模块（AC: 1, 2, 3, 4, 6）
  - [x] 新建 `apps/web/src/components/screen/useInlineAnalysisScheduler.ts`：React hook，接收 `instruments: Array<{instrumentId: number, screenRunId: number}>` 与 `options: {threshold: number, rootMargin: string, maxConcurrent: number}`，返回 per-instrument 的 `InlineAnalysisState = "idle" | "loading" | {data: InlineAnalysisPayload} | {error: string}` 及 `retry(instrumentId)` 动作
  - [x] 内部维护两级状态：a) per-card 状态 Map；b) 一个简单 FIFO 加载队列，受 `maxConcurrent` 节流；c) 单一 `IntersectionObserver` 实例监听所有卡片根 ref，靠近视口时入队
  - [x] 当 `instruments.length < threshold` 时，hook **绕过**调度器直接对全部卡片触发加载（AC2 的 < 20 一次性行为）；threshold 默认 **20**（从 `Epic 3 Story 3.8 AC1/AC2` 的 BDD 常量）
  - [x] 当 props 中 `instruments` 的身份 / 顺序发生变化时（React key 变化或深比较变化），hook **重置**除 `failed` 外的所有 per-card 状态；`failed` 卡片保留 `failed`，但从队列中移除避免自动重试（AC4 + AC6）
- [x] 集成到 `StrategyConfigPanel.tsx`（AC: 1, 2, 3, 5）
  - [x] 在现有 `result-card` 渲染块（Story 3.7 已在此嵌入 `<ResultAnalysisCard>`）外层，用 `useInlineAnalysisScheduler` 驱动 props；把 Story 3.7 里同步的 `fetch(inline-analysis)` 迁移到 hook 中
  - [x] 每张 `result-card` 用 `ref` 注册到 observer；注销必须在 unmount 时正确清理（避免 StrictMode dev 双渲染下的泄露）
  - [x] 从 hook 返回的 `retry(instrumentId)` 传给 `ResultAnalysisCard`，替换 Story 3.7 中"重试加载"按钮的 onClick
- [x] 骨架与文案（AC: 7）
  - [x] `ResultAnalysisCard` 的 `idle` 与 `loading` 状态使用一套**统一的 skeleton 占位**（固定高度 ≈ 两张图表之和，避免滚动跳动）；与 Story 3.7 AC11 的可访问性约束一致（`role="status"` + `aria-label="内联分析加载中"`）
  - [x] 文案优先复用 `apps/web/src/lib/formatters.ts` 与现有 `status-copy` 样式，不新增视觉 primitive
- [x] 常量 / 配置集中化（AC: 1, 3, 7）
  - [x] 新建 `apps/web/src/lib/inlineAnalysisScheduler.ts`：导出 `INCREMENTAL_LOAD_THRESHOLD = 20`、`SCHEDULER_ROOT_MARGIN = "600px 0px"`、`MAX_CONCURRENT_INLINE_LOADS = 4`；模块顶部 docstring 引用 `_bmad-output/planning-artifacts/epics.md#Story 3.8` 为 normative source；
  - [x] `StrategyConfigPanel.tsx` 与调度器 hook **都从该常量模块读**，避免常量漂移
- [x] 失败语义（AC: 5, 6）
  - [x] hook 内把 4xx / 5xx / network error 统一映射到 `{error: string}` 格式，前端只负责展示错误文案（后端错误原文 or 本地化后的回退文案）
  - [x] 已 `failed` 的卡片在 observer 回调里必须**显式跳过**，不进入队列；只有 `retry(instrumentId)` 能把它切回 `loading`
- [x] 测试（AC: 1-8）
  - [x] `apps/web/tests/components/useInlineAnalysisScheduler.test.ts`（新建）：
    - 小列表（5 只）→ 所有卡片立即并发加载；
    - 大列表（50 只）→ 仅首屏可见卡片加载，其余 `idle`；
    - 模拟滚动（`IntersectionObserver` mock）→ 新进入视口的卡片按 FIFO 入队且受 `maxConcurrent` 限制；
    - 排序变化后 `failed` 保留 + 其余重置 + 新视口内优先；
    - 失败卡片不自动重试；`retry(id)` 手动触发重试成功路径；
  - [x] `apps/web/tests/components/StrategyConfigPanel.test.tsx`（扩展现有文件）：
    - 结果数 ≥ 20 时首屏只发起视口内卡片的 `fetch`（断言 `global.fetch` 调用次数 ≤ 视口卡片数）；
    - 结果数 < 20 时 `fetch` 调用次数 = 结果数；
    - 失败卡片渲染"重试加载"按钮且点击后重新 fetch；
    - 失败卡片滚出 / 滚回视口不会触发重复 fetch；
  - [x] `apps/web/tests/e2e/` 若已有 Playwright 或等价 e2e 基线（当前仅 `apps/web/tests/e2e/` 目录存在，不强制要求），本次跳过；
  - [x] 执行：`cd apps/web && npm run lint && npm run build && npm run test`；断言 Story 3.7 的 `ResultAnalysisCard.test.tsx` 与后端 `test_inline_analysis.py` 继续 pass（AC8）

## 开发备注

- **核心反模式**：用户在 v3 patch 中明确反对"手写滚动框架"（Sprint Change Proposal `4.5` 约束原则：`优先复用... 成熟的滚动加载模式`）。本 story 的实现必须：
  - 使用浏览器原生 `IntersectionObserver`（React 19 + Next.js 16 环境下原生可用），**禁止**引入 `react-intersection-observer` / `react-window` / `react-virtualized` 等库——当前列表规模（通常 < 200 只）不需要虚拟化，只需要"按需发起数据请求"；
  - **禁止**自己写滚动事件节流 + `getBoundingClientRect` 手动判定可见性；这是典型的重复造轮子，浏览器已经原生支持；
  - **禁止**为调度器引入 Redux / Zustand / Jotai 等全局状态库；`useInlineAnalysisScheduler` 是 panel-scoped hook，不需要全局状态。
- **为什么保留 `StrategyConfigPanel.tsx` 作为宿主**：3.7 的 `ResultAnalysisCard` 已经以 `article.result-card` 的子区块形式嵌入在 `StrategyConfigPanel.tsx:402-466`。本 story 只需**在同一宿主里**插入调度器 hook，不迁移组件树，降低对 3.7 已绿测试的回归风险。
- **`threshold = 20` 的来源**：Epic 3 Story 3.8 AC1/AC2 的 BDD 文本明确规定阈值为 `20 or more` / `fewer than 20`。该数字被固定为产品契约，**禁止**在实现中改为其他数值或做成用户可配置——如果未来要调整，必须回到 Epic 与 PRD 修订。
- **`rootMargin = "600px 0px"` 选型**：每张 `result-card` 加内联分析后的高度约 500-700px；预加载半屏（≈ 600px）可以让用户正常滚动时感觉不到加载延迟，又不会激进到把视口外 10 张卡片全拉下来违反 AC1。如果实现时测出其他数字更合适，请在 `inlineAnalysisScheduler.ts` 的常量注释中记录选择理由。
- **`maxConcurrent = 4` 选型**：浏览器对同一 origin 的 HTTP/1.1 并发请求上限通常是 6，留 2 条给其它业务请求（health / watchlist toggle 等）。HTTP/2 环境下此限制不严格，但 4 仍是对后端友好的保守值（Story 3.7 的 inline-analysis 端点会触发 lazy refresh，过高并发会放大 provider 请求）。
- **失败不静默重试（AC6）的产品理由**：长列表中一只"当天 quoteSummary 限流失败"的卡片，如果每次滚入视口都自动重试，用户反复滚动会对 Yahoo Finance 上游产生 N 倍请求，很快被封 IP。显式"重试加载"按钮把决定权交还给用户，也为后续在 Story 3.7 provider 层加更 sophisticated 的退避策略留出空间。
- **本 story 严格前端**：**不**新增后端端点、**不**修改数据库 schema、**不**改变 provider 行为。如果实现过程中发现后端 `GET /stocks/{id}/inline-analysis` 在高并发场景下需要 rate-limit / cache，请另写 follow-up story，不在本 story 内 inline 解决（避免 scope 蔓延）。
- **NFR25 的 3 秒约束**：该约束针对的是"首屏渲染 + 可见卡片首批数据"，不是"所有卡片数据到齐"。实现时 skeleton 先行 → 视口内卡片在 3 秒内 loaded 即达标；视口外的卡片什么时候 loaded 不受 NFR25 管束（它们根本不在首屏视野内）。
- **现有 Story 3.7 的同步加载**会被本 story 替换，但**不要**修改 `ResultAnalysisCard` 的 props 形态与内部实现——调度器 hook 负责**把正确的 state prop 喂给它**，内部渲染逻辑不变。这是最小侵入迁移。

### Incremental Load Contract（本 story 锁定的前端契约）

- `INCREMENTAL_LOAD_THRESHOLD = 20`（结果数 ≥ 此值才启用增量加载）
- `SCHEDULER_ROOT_MARGIN = "600px 0px"`（IntersectionObserver 预加载缓冲区）
- `MAX_CONCURRENT_INLINE_LOADS = 4`（同一时刻并发 inline-analysis 请求上限）
- per-card 状态：`"idle" | "loading" | {data: InlineAnalysisPayload} | {error: string}`（discriminated union）
- `failed` 卡片不自动重试；用户点击"重试加载" → 切回 `loading`
- 排序 / 过滤变化 → 重置 `idle` / `loading` / `loaded` 状态，保留 `failed`；新视口内优先入队

### Project Structure Notes

- 新建调度器 hook：`apps/web/src/components/screen/useInlineAnalysisScheduler.ts`
- 新建常量模块：`apps/web/src/lib/inlineAnalysisScheduler.ts`
- 宿主修改：`apps/web/src/components/screen/StrategyConfigPanel.tsx:402-466`（把 3.7 的同步 fetch 替换为调度器 hook 的 state prop 驱动）
- 继承组件（**不修改**）：`apps/web/src/components/screen/ResultAnalysisCard.tsx`（Story 3.7 创建）
- 新建测试：`apps/web/tests/components/useInlineAnalysisScheduler.test.ts`
- 扩展测试：`apps/web/tests/components/StrategyConfigPanel.test.tsx`
- 类型（**不新增**，复用 Story 3.7）：`apps/web/src/lib/types.ts` 的 `InlineAnalysisPayload`、`FiscalYearValuation`
- API paths（**不新增**）：`apps/web/src/lib/apiPaths.ts` 的 `stockInlineAnalysis`

### References

- Story 原文与 BDD 验收：[Source: _bmad-output/planning-artifacts/epics.md:763-796]（Story 3.8 的 6 条 Given/When/Then）
- FR66 / FR67 / NFR25 原文：[Source: _bmad-output/planning-artifacts/prd.md:419-420,486]
- v3 patch 约束原则（禁止自造滚动框架）：[Source: _bmad-output/planning-artifacts/sprint-change-proposal-2026-04-16-page-review-followups.md:80-82,179-182]
- 前置 Story 3.7 契约与组件：[Source: _bmad-output/implementation-artifacts/3-7-inline-screening-result-analysis-cards.md]
- 现有结果卡片宿主：[Source: apps/web/src/components/screen/StrategyConfigPanel.tsx:402-466]
- IntersectionObserver 使用参考（浏览器原生 API，MDN）：https://developer.mozilla.org/en-US/docs/Web/API/Intersection_Observer_API
- Epic 3 retrospective 与 Story 3.6 完成状态：[Source: _bmad-output/implementation-artifacts/epic-3-retro-2026-04-16.md]、[Source: _bmad-output/implementation-artifacts/3-6-expand-stock-detail-chart-history-and-improve-chart-readability.md]
- 架构对增量加载契约的既有约束：[Source: _bmad-output/planning-artifacts/architecture.md:883-888]（后端定义边界；前端不自己推断）

## 开发代理记录

### 使用的代理模型

gpt-5.4

### 调试日志参考

- `npm run test -- ResultAnalysisCard.test.tsx useInlineAnalysisScheduler.test.tsx StrategyConfigPanel.test.tsx apiPaths.test.ts`
- `npm run test`
- `npm run lint`
- `npm run build`

### 完成说明

- 已新增 `useInlineAnalysisScheduler` 与 `inlineAnalysisScheduler.ts` 常量模块，把结果卡片的 inline-analysis 请求从“一次性全发”切换成基于 `IntersectionObserver` 的按视口调度。
- `StrategyConfigPanel` 现已把 scheduler 作为唯一数据入口：小列表 `< 20` 仍即时并发加载，大列表 `>= 20` 只对进入预加载区的卡片发请求，并用 `ref` 注册可见性观察。
- `ResultAnalysisCard` 现在统一支持 `idle` / `loading` / `failed` / `loaded` 四态；失败卡片不会因滚出再滚回而自动重试，只有用户点击“重试加载”才会再次请求。
- 本轮还修了一个宿主层稳定性问题：`inlineAnalysisTargets` 改为稳定 memo，避免每次重渲染都让 scheduler 误判列表顺序变化并重复 reset。

### 文件清单

- apps/web/src/app/globals.css
- apps/web/src/components/screen/ResultAnalysisCard.tsx
- apps/web/src/components/screen/StrategyConfigPanel.tsx
- apps/web/src/components/screen/useInlineAnalysisScheduler.ts
- apps/web/src/lib/apiPaths.ts
- apps/web/src/lib/inlineAnalysisScheduler.ts
- apps/web/tests/components/ResultAnalysisCard.test.tsx
- apps/web/tests/components/StrategyConfigPanel.test.tsx
- apps/web/tests/components/useInlineAnalysisScheduler.test.tsx
- apps/web/tests/lib/apiPaths.test.ts

### 变更日志

- 2026-04-17: Story 3.8 创建（v3 增量补丁）。作为 Story 3.7 的姊妹故事，把 inline-analysis 加载从"同步全部"切换为"按视口增量加载"。严格前端改动，不触碰后端契约与 fundamentals domain。
- 2026-04-17: Story 3.8 开发完成并进入 review。新增 scheduler hook、常量模块、宿主接线、骨架态与失败重试语义，并补齐 hook/宿主测试。
