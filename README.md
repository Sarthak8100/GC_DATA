# Frammer Analytics Dashboard

Frammer Analytics Dashboard is a full-stack analytics workspace for tracking a three-stage video pipeline:

1. Uploads
2. Processed outputs
3. Published outputs

The repository contains:

- A `FastAPI` backend that serves analytics, drill-down, alerting, intelligence, NLP-to-SQL, and custom KPI endpoints
- A `Next.js` frontend that renders dashboard modules for executive summaries, control tower workflows, entity analytics, content/funnel analysis, data quality, video exploration, and NLP-backed KPI creation
- CSV seed data plus a PostgreSQL ingestion script
- An SQLite mirror used by the NLP pipeline for natural-language querying

## Table of Contents

- [Repository Overview](#repository-overview)
- [Architecture](#architecture)
- [Backend](#backend)
- [Frontend](#frontend)
- [Data Model](#data-model)
- [Local Development](#local-development)
- [Deployment Notes](#deployment-notes)
- [Known Constraints](#known-constraints)

## Repository Overview

```text
.
|-- app/
|   |-- main.py                   # FastAPI app entrypoint
|   |-- config.py                 # environment-backed settings
|   |-- database.py               # async SQLAlchemy engine/session setup
|   |-- models/                   # ORM models for dimensions, facts, and custom KPIs
|   |-- routers/                  # API route groups
|   |-- schemas/                  # request/response Pydantic models
|   |-- services/                 # shared analytics and custom KPI logic
|   `-- nlp/                      # local LLM + BM25 + SQLite NLP pipeline
|-- alembic/                      # schema migrations
|-- data/                         # CSV source files for dimensions and fact tables
|-- frontend/                     # Next.js app-router dashboard
|-- ingestion_pipeline.py         # CSV -> PostgreSQL ingestion/upsert script
|-- few_shot_examples.json        # NL -> SQL few-shot examples
|-- pyproject.toml                # Python project metadata
|-- requirements.txt             # Python runtime dependencies
`-- Procfile                      # process definition for deployment
```

## Architecture

At a high level, the system is split into two execution paths:

- Primary analytics path: the FastAPI backend queries the relational warehouse through async SQLAlchemy. This is the source for dashboard metrics, trend charts, funnel analysis, ranking views, alerts, intelligence summaries, and video explorer endpoints.
- NLP/custom KPI path: the backend also exposes an NLP pipeline that converts natural language into SQLite queries over a local mirror database. This path is used for ad hoc analytics and saved custom KPIs.

### End-to-end flow

1. CSV files in `data/` represent a star schema for dimensions and facts.
2. `alembic` and ORM models define the PostgreSQL schema.
3. `ingestion_pipeline.py` reflects the database, normalizes CSV headers, coerces types, and upserts rows into PostgreSQL.
4. `app/main.py` starts FastAPI, initializes ORM-managed tables, and exposes route groups under `/api/v1`.
5. The frontend calls the backend through `frontend/src/lib/api.ts`.
6. Global filters and date ranges are managed client-side and attached to most requests.
7. NLP and custom KPI features call a local LLM-backed planner/generator/verifier pipeline against `frammer_analytics.db`.

## Backend

The backend is the core of the codebase. It does much more than a basic CRUD API: it acts as an analytics query layer, a metrics engine, a funnel model, an anomaly detection surface, an operational intelligence layer, and an NLP-assisted KPI execution service.

### Backend stack

- `FastAPI` for the HTTP application
- `SQLAlchemy 2.x` with async sessions for relational access
- `Alembic` for schema migrations
- `PostgreSQL` as the primary analytics database
- `SQLite` as a local read-oriented mirror for NLP-generated SQL
- `Pydantic v2` and `pydantic-settings` for validation and configuration
- `pandas` and `numpy` for local data wrangling in the NLP/custom KPI path
- an LLM-backed planning and SQL-generation layer for natural-language analytics

### Backend entrypoint

The application starts in [`app/main.py`](/app/main.py).

Important runtime behaviors:

- Registers a FastAPI lifespan hook and calls `init_db()` during startup
- Enables very permissive CORS with `allow_origins=["*"]`
- Mounts all route groups under `/api/v1`, except `/`, `/health`, and `/docs`
- Exposes interactive OpenAPI docs at `/docs`

### Configuration

Environment settings are defined in [`app/config.py`](/app/config.py).

Supported variables:

- `APP_NAME`
- `DEBUG`
- `DATABASE_URL`
- `SYNC_DATABASE_URL`

Operational notes:

- `DATABASE_URL` is required for the primary API to function. If missing, database access fails with HTTP 503.
- Settings are cached with `lru_cache()`, so changes require process restart.

### Database layer

The async database setup lives in [`app/database.py`](/app/database.py).

Key details:

- Uses `create_async_engine()` and `async_sessionmaker`
- Uses `NullPool` for SQLite URLs
- Uses connection pooling for non-SQLite databases
- Imports `app.models` eagerly so SQLAlchemy metadata is fully registered
- Calls `Base.metadata.create_all()` on startup through `init_db()`

This means the app supports two schema-management styles simultaneously:

- `Alembic` for explicit migration history
- ORM startup table creation for safety/convenience

That is convenient for development, but in production it means schema ownership is split across migrations and startup bootstrapping.

### Data model and warehouse shape

The warehouse is modeled as a star schema around a three-stage media pipeline:

- `Fact_Input_Video_Operations`: uploaded inputs
- `Fact_Output_Video_Operations`: processed outputs derived from uploads
- `Fact_Publish_Information`: publish events derived from outputs
- `Fact_Data_Quality`: quality/error log entries

Dimensions:

- `Dim_Channel_Client`
- `Dim_Date`
- `Dim_Input_Type`
- `Dim_Input_Video`
- `Dim_Language`
- `Dim_Output_Type`
- `Dim_Output_Video`
- `Dim_Platform`
- `Dim_User_Team`

Custom persisted objects:

- `custom_kpis`

The ORM models are defined in:

- [`app/models/dimensions.py`](/app/models/dimensions.py)
- [`app/models/facts.py`](/app/models/facts.py)
- [`app/models/custom_kpi.py`](/app/models/custom_kpi.py)

### Why the backend is organized this way

Most route modules do not own large independent data access layers. Instead, they share a common warehouse shape and a consistent request model:

- a date range
- optional dimension filters
- optional granularity or comparison windows
- aggregation/grouping logic built against the same fact chain

This reduces duplication and makes the API feel uniform from the frontend's perspective.

### Shared request/response contracts

Shared schemas live in [`app/schemas/common.py`](/app/schemas/common.py).

Core request types:

- `AnalyticsRequest`: `{ date_range, filters }`
- `TrendRequest`: analytics request + `granularity`
- `ComparisonRequest`: current/previous date ranges + filters
- `MultiDimensionRequest`: one or two dimensions + metric selection
- `TopNRequest`: dimension ranking settings
- `VideoSearchRequest`: search, publication, pagination, sorting
- `ScenarioSimulationRequest`: simulation uplift assumptions

Core response wrapper:

- `ApiResponse`: `{ success, data, meta, message }`

This wrapper is used widely across the backend and matched by the frontend API client.

### Shared analytics service

The most important reusable analytics logic is in [`app/services/analytics_service.py`](/app/services/analytics_service.py).

This module contains the common mechanics that many route groups depend on:

- date filtering through `Dim_Date`
- dimension filtering across the star schema
- granularity mapping for daily/weekly/monthly/quarterly/yearly analysis
- reusable subqueries for:
  - distinct uploaded inputs that later got published
  - distinct processed outputs that got published
  - processed inputs grouped by downstream outputs
- KPI summary computation for uploaded, processed, and published stages
- delta computation between current and previous windows

This service is important because the repository's analytical meaning is not just raw row counting. It encodes business semantics such as:

- an upload can produce multiple outputs
- an output can be published multiple times
- many dashboard metrics care about distinct upstream or downstream entities rather than naive row counts
- date basis matters: upload date, create date, and published date are separate concepts

### Route groups

The backend exposes a broad API surface. All business routes are mounted from [`app/main.py`](/app/main.py).

#### Health and metadata

- `GET /health`
- `GET /api/v1/filters/companies`
- `GET /api/v1/filters/channels`
- `GET /api/v1/filters/users`
- `GET /api/v1/filters/teams`
- `GET /api/v1/filters/languages`
- `GET /api/v1/filters/input-types`
- `GET /api/v1/filters/output-types`
- `GET /api/v1/filters/platforms`
- `GET /api/v1/filters/date-range`

These power the frontend's global filter bar and environment health checks.

#### KPI summary and trends

- `POST /api/v1/kpi/summary`
- `POST /api/v1/kpi/comparison`
- `POST /api/v1/kpi/cards`
- `POST /api/v1/trends/timeseries`
- `POST /api/v1/trends/processed-timeseries`
- `POST /api/v1/trends/published-timeseries`
- `POST /api/v1/trends/comparison`
- `POST /api/v1/trends/cumulative`
- `POST /api/v1/trends/growth-rate`

These routes cover the "what is happening over time?" layer of the product.

#### Dimensional analysis

- `POST /api/v1/dimensions/single`
- `POST /api/v1/dimensions/double`
- `POST /api/v1/dimensions/pivot`
- `POST /api/v1/dimensions/heatmap`

These routes support drill-down and cross-tab exploration across channels, clients, users, teams, languages, input types, output types, platforms, and publication status.

#### Funnel analysis

- `POST /api/v1/funnel/overview`
- `POST /api/v1/funnel/by-dimension`
- `POST /api/v1/funnel/conversion-rates`
- `POST /api/v1/funnel/bottlenecks`

These model the transition from upload to processing to publishing, including drop-off analysis by slice.

#### Entity analytics

- `POST /api/v1/channels/performance`
- `POST /api/v1/channels/comparison`
- `POST /api/v1/channels/trends`
- `POST /api/v1/channels/ranking`
- `POST /api/v1/clients/performance`
- `POST /api/v1/clients/channel-breakdown`
- `POST /api/v1/clients/trends`
- `POST /api/v1/clients/usage-distribution`
- `POST /api/v1/users/performance`
- `POST /api/v1/users/activity`
- `POST /api/v1/users/ranking`
- `POST /api/v1/users/underutilized`
- `POST /api/v1/teams/performance`
- `POST /api/v1/teams/comparison`
- `POST /api/v1/teams/member-breakdown`

These routes answer who is driving activity and where performance differs by organization unit.

#### Content, language, and platform analysis

- `POST /api/v1/content/output-type-mix`
- `POST /api/v1/content/input-type-mix`
- `POST /api/v1/content/type-trends`
- `POST /api/v1/content/input-type-trends`
- `POST /api/v1/content/type-channel-matrix`
- `POST /api/v1/languages/distribution`
- `POST /api/v1/languages/trends`
- `POST /api/v1/languages/by-channel`
- `POST /api/v1/platforms/distribution`
- `POST /api/v1/platforms/trends`
- `POST /api/v1/platforms/by-type`

These routes focus on content formats, multilingual output, and publishing destination behavior.

#### Data quality and explorer workflows

- `POST /api/v1/data-quality/overview`
- `POST /api/v1/data-quality/errors`
- `POST /api/v1/data-quality/errors/details`
- `POST /api/v1/data-quality/trends`
- `POST /api/v1/data-quality/missing-fields`
- `POST /api/v1/data-quality/duplicates`
- `POST /api/v1/data-quality/score`
- `POST /api/v1/videos/search`
- `GET /api/v1/videos/detail/{input_video_id}`
- `POST /api/v1/videos/export`
- `POST /api/v1/videos/bulk-stats`

These routes serve operational investigation use cases: search a video, inspect lifecycle state, export datasets, and review data-quality failure patterns.

#### Comparison, ranking, and alerting

- `POST /api/v1/compare/periods`
- `POST /api/v1/compare/segments`
- `POST /api/v1/compare/benchmarks`
- `POST /api/v1/rankings/top-n`
- `POST /api/v1/rankings/bottom-n`
- `POST /api/v1/rankings/movers`
- `POST /api/v1/alerts/anomalies`
- `POST /api/v1/alerts/thresholds`
- `POST /api/v1/alerts/drop-offs`
- `POST /api/v1/alerts/channel-anomalies`

These routes power leaderboard, benchmark, and exception monitoring views.

#### Operational KPI and productivity layer

- `POST /api/v1/kpi/operational`
- `POST /api/v1/kpi/content-category-performance`
- `POST /api/v1/productivity/user-efficiency`
- `POST /api/v1/productivity/team-efficiency`
- `POST /api/v1/productivity/pipeline-velocity`
- `POST /api/v1/productivity/platform-reach`

This layer goes beyond descriptive metrics and computes higher-order operational measures such as:

- clip multiplication rate
- duration compression ratio
- billable rate
- conversion percentages
- end-to-end days across pipeline stages
- platform reach and saturation
- user/team productivity scores

The output schema for this area is notably rich and centralized in [`app/schemas/common.py`](/app/schemas/common.py).

#### Intelligence / control-tower layer

- `POST /api/v1/intelligence/control-tower`
- `POST /api/v1/intelligence/scenario-simulator`
- `POST /api/v1/intelligence/lost-value`
- `POST /api/v1/intelligence/peer-dna`

This is the most strategy-oriented layer of the backend. It appears designed to convert pipeline analytics into action-oriented recommendations, opportunity scoring, value recovery estimates, scenario planning, and peer clustering.

In product terms, this is the bridge from "what happened?" to "what should we do next?"

#### NLP and custom KPI layer

- `POST /api/v1/nlp-pipeline/query`
- `POST /api/v1/custom-kpis/preview`
- `POST /api/v1/custom-kpis`
- `GET /api/v1/custom-kpis`
- `GET /api/v1/custom-kpis/{custom_kpi_id}`

This layer is materially different from the rest of the backend and deserves special attention.

### NLP pipeline internals

The NLP execution logic lives in [`app/nlp/pipeline.py`](/app/nlp/pipeline.py).

This module implements a multi-stage natural-language-to-SQL workflow:

1. Ensure the SQLite mirror database exists
2. Build schema text dynamically from SQLite `PRAGMA`
3. Load few-shot examples from `few_shot_examples.json`
4. Retrieve the most relevant examples using a BM25 scorer
5. Ask the local LLM to create a structured query plan
6. Ask the local LLM to generate SQL
7. Ask the local LLM again to verify the SQL
8. Execute SQL against SQLite
9. Run sanity checks on the resulting dataframe
10. Generate a one-sentence natural-language answer

Important characteristics:

- It uses SQLite, not PostgreSQL, for NLP execution
- It retries generation with verifier feedback
- It stores intermediate reasoning artifacts such as:
  - query plan
  - planning latency
  - retrieved examples
  - each generation/verification attempt
  - applied filters
  - sanity results

This is valuable because users can inspect not just the result, but also how the system arrived there.

### Custom KPI execution and persistence

Custom KPI logic is implemented in [`app/services/custom_kpi_service.py`](/app/services/custom_kpi_service.py).

The service supports two input modes:

- `natural_language`
- `sql`

For exact SQL mode, the service:

- validates the statement as read-only
- rejects multiple statements
- blocks mutating keywords
- runs `EXPLAIN QUERY PLAN`
- previews results through a bounded subquery
- truncates large outputs to `MAX_RESULT_ROWS`

For natural-language mode, the service:

- delegates to the NLP pipeline
- normalizes and sanitizes the resulting payload
- raises validation errors when the pipeline cannot produce usable SQL

Saved KPI records contain:

- KPI name
- description
- input mode
- original query text
- generated SQL
- result type
- stored result payload
- timestamps

This design means custom KPIs are persisted snapshots of query definitions plus their latest evaluated result payload, not just labels or formulas.

### Ingestion pipeline

[`ingestion_pipeline.py`](/ingestion_pipeline.py) is the bridge from CSV files into PostgreSQL.

It does more than naive inserts:

- reflects live database metadata instead of hardcoding table definitions
- loads tables in foreign-key-safe order
- resolves messy CSV headers through global and per-table aliases
- coerces values by column type
- parses booleans, dates, datetimes, times, numerics, and intervals
- validates required columns
- processes rows in chunks
- performs PostgreSQL upserts using primary-key conflict handling

Important environment variables for ingestion:

- `DATABASE_URL`
- `CSV_DIR`
- `CHUNK_SIZE`
- `STOP_ON_ERROR`
- `SKIP_MISSING_FILES`

This script assumes the target database schema already exists.

### Migrations

Schema history is managed under `alembic/`.

Current migration set:

- initial migration in [`alembic/versions/7485138ea5a3_init.py`](/alembic/versions/7485138ea5a3_init.py)

This migration creates:

- all dimension tables
- all fact tables

Notably, the `custom_kpis` table is not part of that initial migration and is instead created via ORM startup initialization.

### Backend API conventions

There are a few conventions worth understanding before extending the backend:

- Most analytics endpoints are `POST` even when they are read-only, because filters and date windows are sent as structured JSON bodies.
- IDs are usually stored as strings in the backend schema.
- `Dim_Date` is the canonical date bridge for date filtering.
- Duration-heavy metrics are normalized into seconds or hours for responses.
- Many metrics distinguish between totals and "published subset" totals.
- The response envelope is usually `ApiResponse`.

### Extending the backend safely

If you add a new backend metric or route, the safest pattern is:

1. Reuse `AnalyticsRequest`, `TrendRequest`, or `ComparisonRequest` where possible.
2. Reuse `apply_date_filter()` and `apply_dimension_filters()` if the query touches the common star schema.
3. Decide explicitly whether the metric is based on upload date, create date, or publish date.
4. Be careful with duplicate inflation when joining `Fact_Input_Video_Operations`, `Fact_Output_Video_Operations`, and `Fact_Publish_Information`.
5. If the route will be used by the frontend, mirror the contract in `frontend/src/lib/api.ts`.

## Frontend

The frontend lives in `frontend/` and is built with:

- `Next.js` App Router
- `React 19`
- `TypeScript`
- `Recharts`
- custom component primitives plus `shadcn`-style utilities

### Frontend structure

Key files:

- [`frontend/src/app/layout.tsx`](/frontend/src/app/layout.tsx)
- [`frontend/src/components/layout/ClientLayout.tsx`](/frontend/src/components/layout/ClientLayout.tsx)
- [`frontend/src/components/layout/Sidebar.tsx`](/frontend/src/components/layout/Sidebar.tsx)
- [`frontend/src/components/layout/FilterBar.tsx`](/frontend/src/components/layout/FilterBar.tsx)
- [`frontend/src/lib/api.ts`](/frontend/src/lib/api.ts)

### Frontend pages

The navigation exposes these dashboard modules:

- `/executive-summary`
- `/usage`
- `/control-tower`
- `/entities`
- `/explorer`
- `/content`
- `/videos`
- `/quality`
- `/nlp-pipeline`
- `/custom-kpis`

The landing page at [`frontend/src/app/page.tsx`](/frontend/src/app/page.tsx) acts as a module launcher with embedded previews.

### Frontend data flow

The frontend API client centralizes all backend calls in [`frontend/src/lib/api.ts`](/frontend/src/lib/api.ts).

Important details:

- `NEXT_PUBLIC_API_URL` defaults to `http://localhost:8000/api/v1`
- Most calls are thin wrappers around `fetch`
- Shared TypeScript interfaces mirror backend response contracts closely
- Filter state is designed to be global and reused across pages

### Frontend layout model

The UI shell is organized around:

- a persistent sidebar
- a top filter/date bar
- page modules inside a client-side layout

The layout also supports a `?preview=1` mode that hides chrome so pages can be embedded as previews.

## Data Model

The repository encodes a specific business process:

1. An input video is uploaded
2. One or more output videos are created from that input
3. One or more publish events occur from those outputs

That means the fact tables have one-to-many relationships across stages:

- one input -> many outputs
- one output -> many publish events

This is why many metrics must use:

- `COUNT(DISTINCT ...)`
- stage-specific subqueries
- careful join direction

### Core entities

- Channel/client hierarchy
- User/team hierarchy
- Input/output content types
- Languages
- Platforms
- Dates
- Input videos and output videos

### Analytical themes supported by the model

- throughput
- publishing efficiency
- operational bottlenecks
- entity productivity
- content mix
- multilingual behavior
- platform reach
- data quality
- recoverable value
- peer clustering and scenario simulation

## Local Development

### Prerequisites

- Python 3.10+
- Node.js 20+
- npm
- PostgreSQL accessible through an async SQLAlchemy URL
- local LLM/runtime support if using NLP/custom KPI features

### 1. Backend setup

From the repository root:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Create a `.env` file with at least:

```env
APP_NAME=Frammer Analytics API
DEBUG=true
DATABASE_URL=postgresql+asyncpg://USER:PASSWORD@HOST:5432/DBNAME
SYNC_DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/DBNAME
```

Run migrations:

```powershell
alembic upgrade head
```

Start the API:

```powershell
uvicorn app.main:app --reload
```

Backend URLs:

- API root: `http://localhost:8000/`
- Swagger docs: `http://localhost:8000/docs`
- Health check: `http://localhost:8000/health`

### 2. Seed PostgreSQL from CSVs

After the schema exists:

```powershell
python ingestion_pipeline.py
```

### 3. Frontend setup

From `frontend/`:

```powershell
npm install
$env:NEXT_PUBLIC_API_URL="http://localhost:8000/api/v1"
npm run dev
```

Frontend URL:

- `http://localhost:3000`

## Deployment Notes

The repository includes a [`Procfile`](/Procfile):

```text
web: uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080}
```

The deployment model:

- the backend is the primary hosted process
- the frontend is deployed separately behind another service(vercel)

