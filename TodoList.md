## 全盘 Review 待办清单（2026-04-16）

### P0 — 必须立即修复

- [x] **1. config.ini 明文数据库密码** — 已确认 .gitignore 已排除 *.ini，未被 git 跟踪
- [x] **2. 前端零测试覆盖** — 已补 `vitest` 基建、页面/组件/状态/工具层测试与 smoke 测试，`apps/web/tests` 已形成有效覆盖

### P1 — 尽快处理

- [x] **3. 前端类型定义重复** — 已提取到 `src/lib/types.ts`
- [x] **4. 前端缺少 Error Boundary** — 已添加 `ErrorBoundary` 组件
- [x] **5. formatTimestamp 重复定义 5 次** — 已提取到 `src/lib/formatters.ts`
- [x] **6. 后端 backtesting.py 使用 assert** — 已改为 raise ValueError
- [x] **7. CORS 硬编码** — 已改为 `STOCKANALYSE_CORS_ORIGINS` 环境变量

### P2 — 规划中处理

- [x] **8/11. 前后端分页** — 后端 list_backtest_runs 支持 limit/offset
- [x] **9. 前端删除操作无确认** — 已添加 window.confirm
- [x] **10. 前端 API 调用无重试** — 已创建 fetchWithRetry，所有 server-side 页面数据加载已接入
- [x] **12. 后端全局异常处理器和日志** — 已添加
- [x] **13. 后端 N+1 查询** — 已改为 JOIN
- [x] **14. utils/ 遗留代码** — 已确认不在 git 跟踪中

### P3 — 改善体验

- [x] **15. 回测默认日期动态化** — 已改为当前年份
- [ ] **16. 无 CI/CD 流水线** — 暂缓
- [ ] **17. contracts 包为空壳** — 暂缓
- [x] **18. 图表组件优化** — 已用 useMemo
- [x] **19. skip link** — 已添加
- [x] **20. 共享状态管理** — 已创建 WatchlistContext（可供后续组件接入）
- [x] **21. API 路径集中定义** — 已创建 `src/lib/apiPaths.ts`
- [x] **22. API 文档** — FastAPI Swagger 已启用
