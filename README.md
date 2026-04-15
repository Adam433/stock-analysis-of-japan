# stock-analysis-of-japan

## Local Run

### Backend

使用当前有完整数据的数据库启动后端：

```bash
cd apps/api
STOCKANALYSE_DB_PATH=/Users/adam/Code/stockAnalyse/data/stockanalyse.db PYTHONPATH=src /Users/adam/Code/stockAnalyse/.venv/bin/python -m uvicorn stockanalyse_api.main:app --host 127.0.0.1 --port 8000
```

后端地址：

```text
http://127.0.0.1:8000
```

### Frontend

启动前端开发服务器：

```bash
npm run dev:web
```

前端地址：

```text
http://localhost:3000
```

### Verified Local Workflow

下面这组命令已经在当前仓库里实际跑通过，可直接照用：

1. 启动 backend

```bash
cd apps/api
STOCKANALYSE_DB_PATH=/Users/adam/Code/stockAnalyse/data/stockanalyse.db PYTHONPATH=src /Users/adam/Code/stockAnalyse/.venv/bin/python -m uvicorn stockanalyse_api.main:app --host 127.0.0.1 --port 8000
```

2. 在仓库根目录启动 frontend

```bash
cd /Users/adam/Documents/GitHub/stockAnalyse
npm run dev:web
```

3. 可选自检

```bash
curl -s http://127.0.0.1:8000/health/market-data
curl -s http://localhost:3000/
```

期望结果：

- `http://127.0.0.1:8000/health/market-data` 返回 `200 OK`
- `http://localhost:3000/` 可访问，前端日志里会看到 `GET / 200`

### Materialize Derived Facts

补算 `derived_indicator_daily`：

```bash
cd apps/api
STOCKANALYSE_DB_PATH=/Users/adam/Code/stockAnalyse/data/stockanalyse.db PYTHONPATH=src /Users/adam/Code/stockAnalyse/.venv/bin/python -m stockanalyse_api.jobs.materialize_derived_facts
```

说明：

- 这个命令现在会按批次打印进度，格式类似：

```text
[materialize] 125/6421 trade dates through 2001-01-12 | inserted=345678 updated=0
```

- 由于当前使用 SQLite，跑物化时最好先停掉后端服务，否则容易遇到 `database is locked`
- 物化完成后，再重新启动 backend 和 frontend 做页面测试

### Notes

- screening 现在依赖 `derived_indicator_daily`，如果这张表没有补算到最新日期，筛选运行日期会停在当前已物化的最新交易日。
- 如果你误连仓库内默认库 `data/stockanalyse.db`，可能会看到数据不完整；当前测试建议使用上面的 `STOCKANALYSE_DB_PATH`。
