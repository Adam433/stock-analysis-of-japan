# 故事 5.4: 验证回测与已批准 RPS 语义保持一致

状态: review

## 用户故事

作为用户，  
我希望回测输出始终与已批准的 RPS 定义保持一致，  
以便历史评估不会逐渐偏离筛选与图表解释工作流。

## 验收标准

1. 假设已批准的 RPS 业务定义已经冻结，当某个参数集与日期区间的回测被执行时，那么系统能够证明 screening、chart detail 与 backtesting 使用的是同一套 RPS 语义。
2. 假设运营者要调查可疑的回测结果或筛选差异，当他查看回测 run 上下文时，那么他可以直接识别该次回测使用的 RPS 定义版本和所消费的已存数据集范围/指纹，而不需要反向读代码。
3. 假设回测结果在 `/backtests` 工作流中被复盘，当用户或运营者查看最近任务与历史任务时，那么页面会把 RPS 语义版本与数据集上下文作为可读信息展示出来，而不是只显示结果校验和。
4. 假设开发者运行针对回测与筛选的一致性测试，当相同派生事实、参数集和同日范围被比较时，那么 screen 与 backtest 的 RPS 通过语义保持一致。

## 任务 / 子任务

- [x] 为 backtest run 增加可审计的数据集上下文。 (AC: 1, 2, 3)
  - [x] 为 `backtest_runs` 增加能标识“用了哪批已存派生事实”的字段，例如数据集首尾交易日与数据集指纹/校验和。
  - [x] 在 `execute_backtest_run` 中基于实际消费的 `DerivedIndicatorDaily` 计算并持久化这些字段，而不是只保留入选结果校验和。
  - [x] 保持现有 `result_checksum` 语义不变，不要把“结果摘要校验”和“输入数据集指纹”混成同一个字段。
- [x] 在回测读取链路中暴露语义与数据集对齐上下文。 (AC: 1, 2, 3)
  - [x] 更新 `BacktestRunSummary`、`get_backtest_run`、`get_latest_backtest_run`、`list_backtest_runs` 返回 RPS definition-version 与数据集上下文。
  - [x] 明确字段命名和说明，使其表达“本次回测消耗的已存事实范围/指纹”，而不是模糊的运行时状态。
- [x] 在 `/backtests` 页面展示对齐信息。 (AC: 2, 3)
  - [x] 在最近任务和历史任务对比区域显示 RPS definition-version。
  - [x] 展示数据集范围或数据集指纹，让运营者能区分“结果变化来自输入事实变化”还是“结果变化来自参数变化”。
  - [x] 保持现有回测页面的信息层级，不要把页面变成调试控制台；重点是让调查链可读。
- [x] 验证 screen / chart / backtest 的 RPS 语义一致性。 (AC: 1, 4)
  - [x] 为后端补充测试，验证回测执行后会记录 definition-version 与数据集上下文。
  - [x] 补测试验证：同一批派生事实、同一套参数和同一交易日下，screen 的 RPS 通过语义与 backtest 的对应统计不会出现漂移。
  - [x] 运行 backtesting / screening / chart-data 相关测试，以及必要的 lint / migration 校验。

## 开发备注

- `5.5` 已经把 `rps_definition_version` 持久化到 `backtest_runs`，所以本故事不需要再重复“写入 definition-version”本身，而是要把“回测到底消费了哪批已存事实”补成可审计上下文。 [Source: _bmad-output/implementation-artifacts/5-5-persist-rps-definition-version-with-backtest-runs.md]
- 当前 `backtesting.py` 通过 `evaluate_indicator_snapshot` 复用 screening 判定逻辑，这说明算法入口已经统一；缺的不是公式，而是“如何证明这次 run 用的是哪套定义和哪批数据”。 [Source: apps/api/src/stockanalyse_api/services/backtesting.py, apps/api/src/stockanalyse_api/services/screening.py]
- 当前 `result_checksum` 只覆盖回测结果输出，不等于输入数据集指纹。调查差异时，如果只看结果校验和，仍然无法直接判断是否换了底层派生事实。 [Source: apps/api/src/stockanalyse_api/services/backtesting.py]
- `/backtests` 页面已经具备“运行上下文”和“结果复盘”两个区域，是最自然的对齐信息展示位置；本故事应在现有页面内增强，而不是再开单独诊断页。 [Source: apps/web/src/components/backtests/BacktestLaunchPanel.tsx]
- `3.5` 已经把 chart explainability 的边界拉清楚了，因此本故事只需要保证 backtest run 能说清“它和 screening / chart 使用同一语义与同一类权威事实”，不需要再改图表语义。 [Source: _bmad-output/implementation-artifacts/3-5-clarify-rps-chart-semantics-and-explainability-boundaries.md]

## 实施建议

- 可优先考虑 run-level 元数据字段，如 `dataset_trade_date_start`、`dataset_trade_date_end`、`dataset_checksum`，这样最贴合现有 `backtest_runs` 模型。
- 数据集指纹应基于实际被本次回测读取到的 `DerivedIndicatorDaily` 事实集合，而不是基于结果摘要或前端展示字段。
- 一致性测试优先验证“同日 screen 与单日 backtest 的通过语义一致”，这样能直接覆盖契约要求而不需要新建额外调试 API。

## 测试建议

- 至少覆盖：
  - 回测执行后持久化 definition-version 与数据集上下文
  - `get_backtest_run` / `list_backtest_runs` 返回这些字段
  - 同日 screen 与单日 backtest 在相同派生事实和参数下给出一致的 RPS 通过结果
  - `/backtests` 页面 lint 通过，展示不会破坏现有 review 工作流

## 完成说明

- 为 `backtest_runs` 增加了 `dataset_trade_date_start`、`dataset_trade_date_end`、`dataset_checksum`，用于记录本次回测实际消费的已存派生事实范围和指纹。
- `execute_backtest_run` 现在会基于实际读取到的 `DerivedIndicatorDaily` 事实集合计算数据集指纹，同时保留原有 `result_checksum` 作为结果摘要校验。
- `BacktestRunSummary` 和 `/backtests` 页面已展示 RPS definition-version、数据集范围和数据集指纹，调查链不再只能依赖结果校验和。
- 新增一致性测试，覆盖 screen 与单日 backtest 的同日语义对齐，以及 chart detail 与 screen run / 单日 backtest 的同日 trade_date 对齐。
- 复审后补修了两个点：数据集指纹改为使用规范标识（`instrument.id` + `symbol` + `exchange`），并补了 chart detail 对齐测试。

## 验证记录

- `PYTHONPATH=src python3 -m unittest tests.test_backtesting tests.test_screening tests.test_chart_data`
- `PYTHONPATH=src python3 -m alembic -c alembic.ini upgrade head`
- `npm run lint`
- `git diff --check -- apps/api/src/stockanalyse_api/domain/backtests/models.py apps/api/src/stockanalyse_api/services/backtesting.py apps/api/migrations/versions/20260415_0015_add_backtest_run_dataset_context.py apps/api/tests/test_backtesting.py apps/api/tests/test_chart_data.py apps/web/src/components/backtests/BacktestLaunchPanel.tsx _bmad-output/implementation-artifacts/5-4-verify-backtest-alignment-with-approved-rps-semantics.md _bmad-output/implementation-artifacts/sprint-status.yaml`

## 文件清单

- apps/api/src/stockanalyse_api/domain/backtests/models.py
- apps/api/src/stockanalyse_api/services/backtesting.py
- apps/api/migrations/versions/20260415_0015_add_backtest_run_dataset_context.py
- apps/api/tests/test_backtesting.py
- apps/api/tests/test_chart_data.py
- apps/web/src/components/backtests/BacktestLaunchPanel.tsx
- _bmad-output/implementation-artifacts/5-4-verify-backtest-alignment-with-approved-rps-semantics.md
- _bmad-output/implementation-artifacts/sprint-status.yaml
