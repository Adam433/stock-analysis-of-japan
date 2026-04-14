# 故事 3.5: 澄清 RPS 图表语义与可解释性边界

状态: review

## 用户故事

作为用户，  
我希望 RPS 图表能明确区分正式筛选信号与解释性视觉标注，  
以便我不会把一个辅助理解的图表提示误认为真正驱动入选的规则。

## 验收标准

1. 假设个股详情页展示 RPS 历史，当用户同时查看图表与规则拆解时，那么 UI 会明确分开“阈值驱动的正式筛选逻辑”和“仅用于解释的视觉说明”。
2. 假设系统展示 RPS 历史或状态标注，当某个显示状态不属于正式筛选逻辑时，那么产品会把它标记为“仅解释用途”，并且不会暗示它影响了入选结果。
3. 假设用户查看个股详情页的 RPS 面板，当页面呈现“通过/未通过”“最佳 RPS”“阈值线”等信息时，那么这些内容都能回溯到 `rule_breakdown` 或 `indicator_history` 中的权威字段，而不是前端临时推断的新信号。
4. 假设前后端相关测试或人工验证被执行，当开发者检查个股详情体验时，那么“正式判定值”和“解释性文案/图形提示”的边界保持一致，且不会回退到伪造曲线或手写 K 线方案。

## 任务 / 子任务

- [x] 梳理 RPS 图表上的权威信息与解释性信息。 (AC: 1, 2, 3)
  - [x] 盘点 `StockDetailView.tsx` 与 `StockDetailCharts.tsx` 当前展示的 RPS 相关元素，明确哪些属于正式判定信息，哪些只是帮助理解的可视化说明。
  - [x] 以 `_bmad-output/planning-artifacts/rps-semantics-contract.md` 为准，确认当前 MVP 中正式信号仅包括 `rps_50`、`rps_120`、`rps_250`、`best_rps_value`、`rps_threshold` 及其通过判定。
  - [x] 明确 `翻红`、临时走势注释、任何未进入后端契约和测试的图形状态都不能被呈现为正式筛选依据。
- [x] 在个股详情 UI 中把正式信号与解释性展示分层。 (AC: 1, 2, 3)
  - [x] 调整 `StockDetailView.tsx` 的文案、分组或辅助说明，让“规则拆解”与“图表观察”职责分明。
  - [x] 调整 `StockDetailCharts.tsx` 的图例、标题或说明文案，明确阈值线和真实历史序列的意义，同时避免制造新的隐含信号。
  - [x] 如果需要新增图表说明元素，优先使用现有组件结构与轻量文案，不要重新造图表轮子，也不要引入手写 SVG 或自定义 K 线几何。
- [x] 保持前后端契约边界清晰。 (AC: 2, 3)
  - [x] 优先复用现有 `stock detail` payload 中的 `rule_breakdown`、`latest_indicator_snapshot`、`indicator_history` 字段。
  - [x] 如果确实需要新增字段，必须让字段语义指向“说明来源”或“显示边界”，而不是让前端自行推导新的正式判定状态。
  - [x] 禁止前端根据历史序列自行推导新的“官方”RPS 事件标签，除非该概念先被写入契约、PRD、epics 和测试。
- [x] 验证语义边界不会回退。 (AC: 1, 2, 4)
  - [x] 补充或更新与 `stock detail` 相关的测试；如果当前前端缺少自动化测试，至少补充后端契约测试或记录清晰的人工验证步骤。
  - [x] 验证图表说明、规则拆解、阈值展示在同一页面上不会互相矛盾。
  - [x] 确认本故事不会破坏 `3.4` 已完成的成熟图表库方案与真实历史序列展示。

## 开发备注

- `3.4` 已经完成两件关键事情：K 线改用 `lightweight-charts`，RPS 面板改为展示后端持久化的真实历史序列。本故事不是再换图表库，而是把“图上看到的内容到底算不算正式信号”说清楚。 [Source: _bmad-output/implementation-artifacts/3-4-replace-manual-stock-detail-svg-with-lightweight-charts.md]
- 当前 `StockDetailView.tsx` 已展示 `rule_breakdown`、阈值、最佳 RPS 与通过/未通过结论；`StockDetailCharts.tsx` 已展示阈值线和 RPS 50/120/250 真实历史。实现上已经接近正确，但产品语义分层还不够显式。 [Source: apps/web/src/components/stocks/StockDetailView.tsx, apps/web/src/components/stocks/StockDetailCharts.tsx]
- RPS 契约已冻结：正式筛选信号只有 `rps_50`、`rps_120`、`rps_250`、`best_rps_value`、`rps_threshold` 与 `best_rps_value >= rps_threshold`；`indicator_history` 是解释性历史序列，但必须来自后端权威事实；`翻红` 仍不是正式信号。 [Source: _bmad-output/planning-artifacts/rps-semantics-contract.md]
- 架构已经要求 chart / explainability payload 必须保留“权威筛选信号”和“仅解释用途的视觉标注”之间的区分；本故事应直接落实这一条，而不是新增一套并行语义。 [Source: _bmad-output/planning-artifacts/architecture.md]
- PRD 明确要求用户在个股详情里既能看到通过原因，也不能把观察性标注误解成正式规则；这正是本故事的产品目标。 [Source: _bmad-output/planning-artifacts/prd.md]
- 当前仓库里还没有现成的前端 `StockDetailView` / `StockDetailCharts` 自动化测试文件，因此实现时要么补前端测试，要么至少补能守住契约边界的后端测试与人工验证记录，不能只做肉眼修改。 [Source: apps/web/src/components/stocks/StockDetailView.tsx, apps/web/src/components/stocks/StockDetailCharts.tsx, apps/api/tests/test_chart_data.py]

## 实施建议

- 优先在现有 `StockDetailView` 和 `StockDetailCharts` 组件内完成语义澄清，不要新建一整套图表容器。
- “正式信号”建议围绕 `rule_breakdown` 呈现，“解释性信息”建议围绕图例、说明卡片或辅助文案呈现。
- 如果需要显示未来可能扩展的观察性概念，应使用明显的“观察性/仅解释用途”标签，且默认不参与通过/未通过总结。
- 保持当前 `lightweight-charts` 路径，不要回退到手写 SVG，也不要引入第二套成熟图表库。

## 测试建议

- 后端至少确认 `stock detail` payload 仍保持 `rule_breakdown` 与 `indicator_history` 的现有契约。
- 前端如果补测试，优先覆盖“正式筛选结论”和“仅解释用途说明”同时存在时的文案边界。
- 人工验证应至少覆盖：
  - 有完整 `indicator_history` 的个股详情页
  - `indicator_history` 为空时的安全占位状态
  - RPS 通过与未通过两种情形下，规则拆解和图表说明是否一致

## 完成说明

- 在 `StockDetailView.tsx` 中新增“正式筛选信号”与“仅解释用途”两张边界卡片，并把图表区标题改成强调“验证与解释”职责。
- 在 `StockDetailCharts.tsx` 中把图例改为三类说明：正式筛选信号、真实历史序列的解释性用途，以及 `翻红`/临时标注尚未纳入正式规则。
- 没有新增任何前端推导信号，也没有改动图表库；仍然沿用 `lightweight-charts` 与既有 `rule_breakdown` / `indicator_history` 契约。
- 扩展 `test_chart_data.py`，确认个股详情 payload 中最新快照和 `indicator_history` 末尾值保持一致，继续守住“同一权威事实来源”。

## 验证记录

- `PYTHONPATH=src python3 -m unittest tests.test_chart_data`
- `npm run lint`
- `git diff --check -- apps/web/src/components/stocks/StockDetailCharts.tsx apps/web/src/components/stocks/StockDetailView.tsx apps/web/src/app/globals.css apps/api/tests/test_chart_data.py`

## 文件清单

- apps/web/src/components/stocks/StockDetailCharts.tsx
- apps/web/src/components/stocks/StockDetailView.tsx
- apps/web/src/app/globals.css
- apps/api/tests/test_chart_data.py
