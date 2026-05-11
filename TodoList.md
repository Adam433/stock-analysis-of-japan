# stockAnalyse TodoList：低频策略回测、GA 参数搜索、前端操作台

## 核心结论

- 正式操作入口统一放在 `apps/web`。回测、参数优化、GA 找策略是同一个前端里的不同路径，不是多个互相割裂的前端。
- FastAPI 后端负责数据、回测、参数优化、GA 编排和结果持久化；前端只负责启动任务、查看进度、对比结果、保存策略。
- FastAPI 内置 `dashboard.html` 暂时保留为内部实验台和过渡页面，但不是长期主入口。
- GA 对本项目可用，但只能作为“低频策略参数搜索器”，不能照搬超短线系统的自由策略生成方式。
- 当前策略仍以美股低频、财务质量、RPS、宽松 K 线形态、市场过滤、相对 SPY 强势为核心。

## 不做或暂不做

- 不做 5m K 线、多周期重采样、分钟级持仓或日内交易。
- 不把 RSI/MACD/custom alerts 作为第一阶段 GA 搜索基因。
- 不让 GA 自由发明全新交易规则；第一版只在现有策略框架内调参数。
- 不让 LLM 当裁判；LLM 种子生成和停滞期建议暂缓，等纯 GA 闭环稳定后再评估。
- 不重写一套回测引擎；GA 必须复用现有 `run_cup_handle_rps_backtest` / `optimization_backtest` 能力。
- 不急着删除 `dashboard.html`；等 `apps/web` 覆盖常用功能后再决定。

## Phase 1：策略参数 Schema 与 Evaluator

- [x] 定义 `StrategyParameterSet` schema，覆盖财务、RPS、K线形态、市场过滤、相对 SPY 强势、止损、退出、持仓上限、仓位大小。
- [x] 给策略参数 schema 加版本号，例如 `strategy_schema_version`，避免旧实验结果和新字段混用。
- [x] 把参数 normalization/hash 逻辑从普通优化中抽成可复用模块，供参数优化和 GA 共用。
- [x] 固定 GA evaluator 输入输出：输入参数组合和训练/验证窗口，输出 metrics、trade-level SPY alpha、账户曲线摘要、失败原因。
- [x] 固定 `spy_alpha` 口径：主指标是实际成交单笔相对同持有期 SPY 的平均超额收益；辅助指标是跑赢 SPY 交易比例、账户回撤、交易样本数和跨窗口一致性。
- [x] 增加 evaluator 级别 benchmark 检查：SPY 数据不足时参数组合不可评分。

## Phase 2：GA 实验记录模型

- [x] 新增 `ga_runs`：记录 market、窗口、目标函数、种群规模、代数、随机种子、状态、进度、最佳个体。
- [x] 新增 `ga_individuals`：记录 generation、parameter_hash、parameters_json、fitness、父代、变异说明、关联的 optimization/backtest result。
- [x] 新增 GA 事件记录：记录每代选择、交叉、变异、淘汰、停滞、失败重试。
- [x] GA 结果复用现有 optimization metrics 格式，避免出现第二套结果解释逻辑。
- [x] 写迁移和基础测试，确认已有库升级和空库升级都可用。

## Phase 3：最小可用 GA

- [x] 第一版 GA 只做 Python 后端任务，不先做前端页面。
- [x] 初始种群来自当前人工候选：`quality_light_no_valuation`、`value_quality`、`growth_ocf`、市场过滤、相对 SPY 强势组合。
- [x] 第一版基因范围限制在已有有效区域：RPS 70/80，窗口 `120+250` 和 `50+120+250`，RPS exit 75/80/85，止损 6/8/10%，K线 `none/loose_no_prior`，市场过滤 `none/spy_200ma/spy_50_200ma`，相对强势 `none/spy_120d/spy_250d`。
- [x] 实现 selection：优先选择高 fitness 且跨窗口稳定的个体。
- [x] 实现 crossover：只在同 schema 参数间交叉，例如财务组、RPS 组、K线组、风控组分别交换。
- [x] 实现 mutation：小概率调整单个维度，例如 RPS 阈值、退出阈值、市场过滤、相对强势窗口。
- [x] 实现 elite 保留、参数去重、early stop。
- [x] 默认单 worker 或低 worker，并支持 `max_tasks_per_child`，控制内存和虚拟内存风险。当前最小 GA 为单进程执行，暂不创建子进程。

## Phase 4：防过拟合验证

- [x] GA fitness 不能只看 2023-2026，应至少使用 3 个滚动窗口。
- [x] 训练窗口用于进化，验证窗口用于评分，最终保留一个完全未参与进化的 holdout 窗口。
- [x] fitness 拆成：验证 SPY alpha、训练/验证一致性、最大回撤、样本数、交易容量、年度稳定性。
- [x] 每个窗口至少要有可解释的交易样本，且不能只靠某一年或某几笔大牛股贡献大部分收益。
- [x] 每代生成报告：最佳个体、平均 fitness、多样性、重复率、失败率、每个参数维度贡献趋势。

## Phase 5：性能与缓存

- [x] GA evaluator 复用现有 worker cache：交易日、RPS 候选、财务、杯柄事件、相对强势、未来行情。
- [x] 设计宽松候选池预物化：GA 可先缓存每日宽松 RPS 候选池，再按具体 RPS 参数内存过滤；默认设置交易日上限，避免长窗口内存放大。
- [x] 对相同窗口、相同核心筛选条件建立 cache key，避免每个个体完整逐日重筛。
- [x] 记录每代耗时、单个个体耗时、缓存命中率、内存占用。
- [x] GA 结果只摘要落库，剥离完整资金曲线和回测明细，避免扩大实验时数据库和内存被大 JSON 放大。

## Phase 6：前端整合

- [x] 保留 `apps/web`，定位为正式操作台。
- [x] 在 `apps/web` 中用不同路径承载工作流：`/backtests` 管回测，`/experiments` 管 GA/参数优化实验，`/screen` 管日常筛选，`/watchlist` 管观察列表。
- [x] 先做只读实验列表：展示 optimization run 和 GA run 状态、进度、最佳结果。
- [x] 再迁移现有美股参数优化结果列表、详情、删除、重跑、保存预设到 `apps/web`。
- [ ] 最后新增 GA 实验创建页：选择种群规模、代数、窗口、启动/取消、查看每代进度。

## 当前执行顺序

1. Phase 1：先做策略参数 schema 和 evaluator 边界。
2. Phase 2：再做 GA 实验表和持久化。
3. Phase 3：实现最小 GA，单 worker 小种群跑通。
4. Phase 4：加入 walk-forward 和 holdout，防止拟合旧数据。
5. Phase 5：性能缓存已做第一轮，后续根据更大样本运行情况继续收窄瓶颈。
6. Phase 6：下一步新增 GA 实验创建页，并继续把旧 dashboard 的常用操作收敛到 `apps/web`。
