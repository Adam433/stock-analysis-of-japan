## 全盘 Review 待办清单（2026-04-16）

### P0 — 必须立即修复

- [x] **1. config.ini 明文数据库密码** — 已确认 .gitignore 已排除 *.ini，未被 git 跟踪
- [x] **2. 前端零测试覆盖** — 已补 `vitest` 基建、页面/组件/状态/工具层测试与 smoke 测试，`apps/web/tests` 已形成有效覆盖
- [x] **23. 运行库 schema 落后导致 `/screen` 与个股详情 500** — 已对运行中的 SQLite 执行 alembic upgrade，补齐 `strategy_configurations.selected_rps_windows` 等列，恢复 `/screen` 与个股详情链路
- [x] **24. 筛选页与个股详情页需在修复 schema 后回归验证** — 已复验 `/screen/runs/latest` 与个股详情接口恢复 200，前后端相关测试通过

### P1 — 尽快处理

- [x] **3. 前端类型定义重复** — 已提取到 `src/lib/types.ts`
- [x] **4. 前端缺少 Error Boundary** — 已添加 `ErrorBoundary` 组件
- [x] **5. formatTimestamp 重复定义 5 次** — 已提取到 `src/lib/formatters.ts`
- [x] **6. 后端 backtesting.py 使用 assert** — 已改为 raise ValueError
- [x] **7. CORS 硬编码** — 已改为 `STOCKANALYSE_CORS_ORIGINS` 环境变量
- [x] **25. 首页导航样式与 workflow 页面不一致** — 首页已切换为与 workflow 页面一致的 `top-nav` 顶部导航
- [x] **26. RPS 规则移除“最少满足条数”参数** — 已同步移除前后端接口、筛选判定与展示文案，改为所有纳入筛选的 RPS 周期都必须满足阈值
- [x] **27. 筛选执行不应强依赖“已保存参数集”** — 已通过 PRD FR5/FR62/FR63 与 Story 2.1 修订落地：双按钮（试跑 / 保存）解耦
- [x] **28. 当前版本 / 参数集版本的使用逻辑不清晰** — 已通过 PRD FR62 与 Story 2.3 修订落地：每次 run 持久化独立参数 snapshot，含 source 字段（ad_hoc_form / saved_configuration:{id} / legacy）
- [x] **29. 观察列表应自动沉淀筛选上下文** — 已通过 PRD FR64 与 Story 4.1/4.2 修订落地：watchlist 自动按引用方式附带 screen_run_id + screen trade date
- [x] **30. 筛选与回测的参数持久化语义需要拆分** — 已通过 Story 2.1（试跑/保存解耦）+ Story 5.6 锚点（回测以 screen_run_id 为入口）共同落地
- [x] **32. 筛选结果区需要升级为分析卡片流** — 已通过 PRD FR65/FR67 与新增 Story 3.7 落地：1 年 K 线 + 财年净利润柱状图 + PE/PB 同图
- [x] **33. 筛选结果区需要支持滚动增量加载** — 已通过 PRD FR66 与新增 Story 3.8 落地：≥20 启用增量、<20 一次性加载、失败显式重试
- [x] **34. 回测页“启动回测 / 执行最新任务”语义必须重构** — 已通过 PRD FR72 与 Story 5.1 修订落地：单步启动、debounce、failed-recoverable 状态
- [x] **35. 当前“回测”实际是历史条件回放，不是收益回测** — 已通过 PRD FR41 修订 + Story 5.6 锚点 + 旧 run 标 legacy_condition_hit 落地
- [x] **36. 收益回测 MVP 需要明确建仓口径** — 已通过 PRD FR68 与 Story 5.2 落地：T+1 开盘 + 停牌/退市/无效开盘的 deferral 与 exclusion 规则
- [x] **37. 收益回测 MVP 需要明确组合规则** — 已通过 PRD FR69 与 Story 5.2/5.6 落地：等权 + cap 20 + RPS 排序截断 + 不再调仓
- [x] **38. 收益回测 MVP 需要明确持有与卖出规则** — 已通过 PRD FR71 与 Story 5.2 落地：持有周期 20 交易日（可调）+ 持有期满次日开盘平仓
- [x] **39. 收益回测 MVP 必须包含止损规则** — 已通过 PRD FR70 与 Story 5.2 落地：默认 -8%、按日 adjusted close 判定、次日开盘成交、不复投
- [x] **40. 回测结果输出需要改成收益指标** — 已通过 PRD FR45 与 Story 5.3 落地：组合收益、胜率（>0 占比）、最大回撤、equity curve、单标的收益分布

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
- [ ] **31. 筛选页需要补一轮信息架构收口** — 当前页面对版本号、保存动作、运行动作、结果所用参数的说明不足，用户难以验证系统到底按哪套参数执行
