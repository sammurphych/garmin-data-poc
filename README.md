# Garmin Data Stack POC

A trial of **dlt → MotherDuck (DuckDB) → SQLMesh → Cube Core → Evidence + MCP**. The orchestrator is **Prefect Cloud (free)**, and there are no GitHub Actions. Everything runs on free tiers.

```
Prefect Cloud  (cron 06:00 UTC, Prefect Managed work pool, code cloned from git)
  └─ garmin-daily flow
       1. extract_load  dlt: Garmin Personal API → MotherDuck  health.raw_garmin.*
       2. transform     SQLMesh run: staging.* views → marts.* tables (+ audits)
       3. refresh_bi    POST Netlify build hook → Evidence site rebuilds from MotherDuck
Local
  Cube Core (docker) over health.marts → REST/SQL API → MCP server → Claude
```

| Path | What it does |
|---|---|
| `ingest/` | dlt sources: Garmin Personal API (`daily_summaries`, `activities`, `sleep`, `hrv`) and Withings (`measures`: weight and body composition) |
| `transform/` | SQLMesh project (`config.py` picks the gateway: `local` or `motherduck`) |
| `flows/garmin_daily.py` | Prefect flow; `prefect.yaml` is the deployment and schedule |
| `semantic/` | Cube Core models and views, docker compose, MCP server |
| `dashboards/` | Evidence project; `netlify.toml` builds it on Netlify |

## Secrets

| Secret | Where | Used by |
|---|---|---|
| Garmin personal API key | Prefect Secret `garmin-api-key` | dlt |
| MotherDuck token (read/write) | Prefect Secret `motherduck-token` | dlt, SQLMesh |
| Netlify build hook URL | Prefect Secret `evidence-build-hook` | refresh_bi |
| SQLMesh state database URL (Postgres, e.g. Neon) | Prefect Secret `sqlmesh-state-db-url` | SQLMesh |
| Withings client id / secret | Prefect Secrets `withings-client-id`, `withings-client-secret` | dlt |
| Withings refresh token (rotates every run) | Prefect Secret `withings-refresh-token`, local runs too | dlt |
| GitHub fine-grained token (only if the repo is private) | Prefect Secret `github-token` | git clone in `prefect.yaml` |
| MotherDuck dashboards token (read-only on Business plan, a separate regular token on free) | Netlify env `EVIDENCE_SOURCE__motherduck__token` | Evidence build |
| Everything above, for dev | `.env` (gitignored, copy from `.env.example`) | local runs, Cube, MCP |

## Setup

**0. Tools.** Install `uv` (`brew install uv`), Node 20+ (`brew install node`) and Docker (Docker Desktop or `brew install colima docker docker-compose`). Then:
```bash
cp .env.example .env      # fill in GARMIN_API_KEY
uv sync --all-groups
```

**1. Garmin key.** In Garmin Connect (web) open the Personal API settings, accept the terms and create a key (1-year expiry). The key is shown only once. Garmin emails you about a week before it expires.

**2. Load locally into `./health.duckdb`** (`WAREHOUSE=local`):
```bash
uv run python -m ingest.pipeline
uv run dlt pipeline garmin_local show         # browse loaded tables
uv run dlt pipeline garmin_local schema       # check the real column names
```
The staging SQL assumes dlt's flattened names, such as `heart_rate__resting_value` and `sleep_scores__overall__value`. If a field never appears in your data, dlt won't create that column. Adjust `transform/models/staging/*.sql` to match the schema output.

**3. SQLMesh:**
```bash
cd transform
uv run sqlmesh create_external_models   # typed external models for raw_garmin.*
uv run sqlmesh plan                     # apply to prod (backfills from GARMIN_START_DATE)
uv run sqlmesh fetchdf "select * from marts.fct_daily_health order by 1 desc limit 5"
```

**4. Switch to MotherDuck.** Create a free account and a read/write token, plus a second token for Evidence and Cube. Read-only (Read Scaling) tokens need the Business plan; on the free plan use a separate regular token so you can revoke it on its own. Set `WAREHOUSE=motherduck` and `MOTHERDUCK_TOKEN` in `.env`. Then run steps 2 and 3 again (`sqlmesh plan` on the motherduck gateway).

**5. Prefect Cloud:**
```bash
uv run prefect cloud login
uv run prefect work-pool create managed-pool --type prefect:managed
# create Secret blocks (UI: Blocks → Secret), or:
uv run python -c "from prefect.blocks.system import Secret; Secret(value='...').save('garmin-api-key')"
#   ... likewise motherduck-token, evidence-build-hook, github-token
```
Push this repo to GitHub and set `repository:` in `prefect.yaml`. If the repo is public, delete the `access_token` line. Then:
```bash
uv run prefect deploy --all
uv run prefect deployment run garmin-daily/garmin-daily   # run once now
```
The cron runs daily at 06:00 UTC; change `timezone` in `prefect.yaml`. Prefect Managed free tier has a monthly compute-hour allowance, and this job takes a few minutes per day.

> SQLMesh model changes are applied with `sqlmesh plan` from your laptop. The scheduled flow only runs `sqlmesh run` (new intervals and audits) for what is already in prod.

**6. Evidence on Netlify.** Locally:
```bash
cd dashboards && npm install
EVIDENCE_SOURCE__motherduck__token=<dashboards token> npm run sources && npm run dev
```
On Netlify, choose *Add new site → Import from Git* and pick this repo; `netlify.toml` sets the build. Add env var `EVIDENCE_SOURCE__motherduck__token`. Under *Build hooks*, create a hook and save its URL as the Prefect Secret `evidence-build-hook`.

**7. Cube and MCP:**
```bash
cd semantic && docker compose --env-file ../.env up -d   # Playground: http://localhost:4000
psql "postgresql://cube:$CUBEJS_API_SECRET@localhost:15432/db" -c "select measure(avg_resting_hr) from health"
cd .. && claude mcp add garmin-cube -- uv run --group mcp python semantic/mcp_server.py
```
Then ask Claude: *"What was my average resting HR last week vs the 4 weeks before?"*

**8. SQLMesh state in Postgres.** Create a free [Neon](https://neon.tech) project and copy its connection string into `.env` as `SQLMESH_STATE_DB_URL`. Move the existing state across, then store the URL for Prefect:
```bash
cd transform
SQLMESH_STATE_DB_URL= WAREHOUSE=motherduck uv run sqlmesh state export -o ../sqlmesh_state.json   # from MotherDuck
WAREHOUSE=motherduck uv run sqlmesh state import -i ../sqlmesh_state.json                        # into Postgres
WAREHOUSE=motherduck uv run sqlmesh plan                                                          # expect "no changes"
```
The `local` gateway keeps its state in `health.duckdb`.

**9. Withings.** At [developer.withings.com](https://developer.withings.com) create an application (public API integration) with callback URL `http://localhost:8765/callback`. Put its client id and secret in `.env`, then authorise once and load:
```bash
uv run python -m ingest.withings_auth        # browser login; saves the refresh token to Prefect
uv run python -m ingest.pipeline withings    # full history on the first run
cd transform && uv run sqlmesh create_external_models && uv run sqlmesh plan
```
Save `withings-client-id` and `withings-client-secret` as Prefect Secrets, then `prefect deploy --all`.

## Notes and limits
- The Garmin Personal API is read-only and covers your own data only. Rate limits aren't published, so the client pauses 0.5s between calls. Max ranges are 1 year (daily summaries), 31 days (sleep) and 28 days (HRV, conservative).
- Every run re-pulls the last 3 days, because Garmin revises recent days. The merge on natural keys keeps rows unique, and SQLMesh `lookback 3` reprocesses the same days.
- SQLMesh state for the motherduck gateway is in Postgres when `SQLMESH_STATE_DB_URL` is set, and in MotherDuck otherwise.
- Withings rotates its refresh token on every use, so the token lives only in the Prefect Secret `withings-refresh-token`. Laptop and cloud runs both read and update it there. Withings asks for at most one poll per user every 10 minutes.
- Cube runs locally. To keep it online for free, deploy the same image to Render's free tier (it sleeps when idle).
