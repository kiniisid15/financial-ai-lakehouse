# Financial AI Lakehouse

Async market-data ingestion pipeline that fetches:
- **Intraday OHLCV prices** (Yahoo Finance via `yfinance`)
- **Ticker news headlines** (Yahoo RSS)

and stores them in **PostgreSQL + pgvector** for downstream analytics or AI use-cases.

---

## 1) What this project does

This project continuously ingests financial data for configured tickers and writes to:

- `prices` table (time-series OHLCV)
- `news` table (ticker-level news metadata/content)

It is designed as a foundation for a financial AI stack where embeddings search can be added later (embedding fields already exist in schema and are currently inserted as `None`).

---

## 2) Tech stack

- **Python** (async orchestration with `asyncio`)
- **SQLAlchemy Async** + **asyncpg**
- **PostgreSQL 16** + **pgvector**
- **Docker Compose** (DB + pgAdmin)
- Data sources:
  - `yfinance` (prices)
  - Yahoo Finance RSS (news)

---

## 3) Repository structure

```text
.
├── app/
│   ├── create_tables.py      # creates extension + tables
│   ├── database.py           # async engine/session config
│   └── models.py             # SQLAlchemy models: Price, News
├── db/
│   └── init.sql              # indexes for upsert/query performance
├── fetchers/
│   ├── price_fetcher.py      # fetch_prices()
│   └── news_fetcher.py       # fetch_news()
├── docker-compose.yml        # postgres + pgadmin services
├── main.py                   # async ingestion entrypoint
└── .env                      # local environment config (not committed)
```

---

## 4) Prerequisites

- Docker + Docker Compose
- Python 3.11+ (3.13 works)
- pip

Install Python dependencies in your environment :

```bash
pip install sqlalchemy asyncpg python-dotenv requests yfinance pandas pgvector
```

---

## 5) Environment configuration

Create `.env` in project root with at least:

```env
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_DB=financial_ai

PGADMIN_EMAIL=admin@example.com
PGADMIN_PASSWORD=admin123

TICKERS=AAPL,MSFT
```

> Notes:
> - DB is expected at `localhost:5433`.
> - `TICKERS` is comma-separated and parsed into a Python list at startup.

---

## 6) Local setup (0 → running)

### Step 1: Start infrastructure

```bash
docker compose up -d
```

Services:
- Postgres container: `financial_pg` on port `5433`
- pgAdmin container: `financial_pgadmin` on port `5050`

### Step 2: Create tables and extension

```bash
python app/create_tables.py
```

This creates:
- `vector` extension (if missing)
- all SQLAlchemy tables from `app/models.py`

### Step 3: Apply indexes (optional but recommended)

```bash
docker exec -i financial_pg psql -U <POSTGRES_USER> -d <POSTGRES_DB> < db/init.sql
```

### Step 4: Run ingestion

```bash
python main.py
```

---

## 7) Runtime behavior

`main.py` flow:

1. `startup` log
2. Load tickers from `.env`
3. `wait_for_db()` retries DB connection every 2s until ready
4. Concurrent fetch:
   - `fetch_prices(tickers)`
   - `fetch_news(ticker)` for each ticker
5. Upsert into DB with `AsyncSession`
6. Sleep and repeat (current loop interval in code)

Expected logs include:
- startup
- db ready
- rows fetched
- rows inserted
- errors/tracebacks (if any)

---

## 8) Data model snapshot

### `prices`
- `symbol`, `ts` (unique pair for upsert)
- `open`, `high`, `low`, `close`, `volume`
- `embedding` (`Vector(1536)`, currently set to `None` during ingest)

### `news`
- `symbol`, `title`, `content`, `published_at`, `source`
- `embedding` (`Vector(1536)`, currently set to `None`)

Indexes currently present in `db/init.sql`:
- `ux_prices_upsert` on `(symbol, ts)`
- `ux_news_symbol_title_published` on `(symbol, title, published_at)`
- `idx_prices_symbol_time` on `(symbol, ts DESC)`

---

## 9) Known operational notes (troubleshooting) 

### A) DB retry loop is active
If Postgres is not ready, ingestion keeps retrying every 2 seconds by design.

### B) News upsert SQL failure observed
Recent terminal output showed a failure:

`syntax error at or near "DO"`

This occurs when `DO UPDATE` executes without a valid `ON CONFLICT (...)` clause in the final SQL. If you hit this:

1. Stop old running `python main.py` terminals.
2. Ensure latest `main.py` is running.
3. Confirm the generated SQL includes:
   - `ON CONFLICT (url) DO UPDATE ...` **or**
   - `ON CONFLICT (symbol, title, published_at) DO UPDATE ...`

### C) Frequent `database "user" does not exist` in Postgres logs
Usually caused by external clients/health checks attempting wrong DB names. Validate connection strings in your tools.

---

## 10) Current status of project

- ✅ Dockerized DB stack works
- ✅ Async DB connectivity works
- ✅ Price fetch + price upsert works
- ✅ News fetch works
- ⚠️ News upsert path needs validation against runtime SQL generation in long-running cycle
- ⚠️ Embedding pipeline is placeholder (`None`) and should be replaced by a real embedding service

---

## 11) Next professional improvements 

- Add migrations (`alembic`) instead of ad-hoc DDL
- Add unit/integration tests for fetchers + SQL upsert builders
- Move raw SQL upserts to SQLAlchemy PostgreSQL `insert().on_conflict_do_update()` for safety
- Add structured logging + metrics
- Add retry/backoff and circuit breaker per external source
- Add scheduler controls (graceful shutdown, configurable interval)
- Add embedding generation worker and vector similarity search endpoints

---

## 12) License

This repository includes a `LICENSE` file at root. Review it for usage/redistribution terms.
