# Garmin Data Stack POC

A trial of **dlt → MotherDuck (DuckDB) → SQLMesh → Cube Core → Evidence + MCP**. The orchestrator is **Prefect Cloud (free)**, and there are no GitHub Actions. Everything runs on free tiers.

```
Prefect Cloud  (cron 06:00 UTC, Prefect Managed work pool, code cloned from git)
  └─ garmin-daily flow
       1. extract_load  dlt: Garmin Personal API, Withings, Open-Meteo weather → MotherDuck  health.raw_*
       2. transform     SQLMesh run: staging.* views → marts.* tables (+ audits)
       3. refresh_bi    POST Netlify build hook → Evidence site rebuilds (Mon + Thu only, to save Netlify credits)
Render (free web services)
  ├─ health-cube: Cube Core over health.marts → REST API (Docker image from semantic/Dockerfile)
  └─ health-mcp:  MCP server over HTTPS, OAuth sign-in → claude.ai custom connector
Local
  MCP server (semantic/mcp_server.py) → Claude Code; local_chat.py → llama.cpp
```

| Path | What it does |
|---|---|
| `ingest/` | dlt sources: Garmin Personal API (`daily_summaries`, `activities` with `split_summaries`, `activity_samples`, `sleep`, `hrv`, `vo2max`) Withings (`measures`: weight and body composition) and Open-Meteo (`hourly` weather for the area each activity starts in) |
| `transform/` | SQLMesh project (`config.py` picks the gateway: `local` or `motherduck`) |
| `flows/garmin_daily.py` | Prefect flow; `prefect.yaml` is the deployment and schedule |
| `semantic/` | Cube Core models and views (`health`, `training`, `body`), Render Dockerfile, MCP server, local-model chat |
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

**0. Tools.** Install `uv` (`brew install uv`) and Node 20+ (`brew install node`, needed for Evidence). Docker isn't needed locally, because Render builds the Cube image. Then:
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

**7. Cube on Render, and the MCP server.** Cube runs on Render's free tier from `render.yaml` and `semantic/Dockerfile` (it sleeps when idle, so the first query after a while takes 30-60 s). In Render choose *New → Blueprint*, pick this repo, and set the secret env vars it asks for: `CUBEJS_DB_DUCKDB_MOTHERDUCK_TOKEN` and `motherduck_token` (both the dashboards token) and `CUBEJS_API_SECRET` (same value as in `.env`). Then set `CUBE_API_URL=https://<service>.onrender.com/cubejs-api/v1` in `.env`, using the URL on the service page (Render may add a suffix such as `-90ae`). The MCP server is registered for Claude Code by `.mcp.json` in this folder; open the project in Claude Code and approve `health-cube` when asked. Then ask Claude: *"What was my average resting HR last week vs the 4 weeks before?"* The views are `health` (Garmin daily wellness), `training` (activities) and `body` (Withings weight).

**8. SQLMesh state in Postgres.** Create a free [Neon](https://neon.tech) project and copy its connection string into `.env` as `SQLMESH_STATE_DB_URL`. Move the existing state across, then store the URL for Prefect:
```bash
cd transform
SQLMESH_STATE_DB_URL= WAREHOUSE=motherduck uv run sqlmesh state export -o ../sqlmesh_state.json   # from MotherDuck
WAREHOUSE=motherduck uv run sqlmesh state import -i ../sqlmesh_state.json                        # into Postgres
WAREHOUSE=motherduck uv run sqlmesh plan                                                          # expect "no changes"
```
The `local` gateway keeps its state in `health.duckdb`.

**9. Withings.** At [developer.withings.com](https://developer.withings.com) create an application (public API integration). Withings rejects localhost callbacks, so register the Netlify page `https://<your-site>.netlify.app/withings-callback.html` (from `dashboards/static/`) and set the same address as `WITHINGS_REDIRECT_URI`. Put the client id and secret in `.env`, then authorise once and load. After you approve, copy the callback page's address; the script watches the clipboard and exchanges the code within Withings' 30-second limit:
```bash
uv run python -m ingest.withings_auth        # browser login; saves the refresh token to Prefect
uv run python -m ingest.pipeline withings    # full history on the first run
cd transform && uv run sqlmesh create_external_models && uv run sqlmesh plan
```
Save `withings-client-id` and `withings-client-secret` as Prefect Secrets, then `prefect deploy --all`.

**10. Local model (optional).** Ollama needs macOS 14+, so this uses [llama.cpp](https://github.com/ggml-org/llama.cpp)'s `llama-server`, which has an OpenAI-compatible API with tool calling. Download the `bin-macos-x64` (Intel) or `bin-macos-arm64` build from its releases into `~/.local/llama.cpp/`, then:
```bash
~/.local/llama.cpp/llama-b12262/llama-server -hf unsloth/Qwen3-4B-Instruct-2507-GGUF:Q4_K_M --jinja -c 8192 --port 8080
uv run --group mcp python semantic/local_chat.py      # in another terminal
```
The first start downloads the model (~2.5 GB). `local_chat.py` gives the model the same Cube `query` tool the MCP server gives Claude, so the two can be compared on the same questions. On a CPU-only laptop expect tens of seconds per answer.

**11. claude.ai (browser and app).** `render.yaml` also defines `health-mcp`, which runs `semantic/mcp_remote.py`: the same two read-only tools over HTTPS, behind a sign-in only you can pass. In Render, open the Blueprint and *Sync* it so the new service is created. Set `CUBE_API_URL` and `CUBEJS_API_SECRET` to the same values as in `.env`. Render generates the other four values. Then in claude.ai (use a personal account; a work account's admins control connectors and can see chats) go to *Settings → Connectors → Add custom connector*:
- **URL:** `https://<health-mcp service>.onrender.com/mcp`
- **Advanced settings:** OAuth Client ID = `MCP_OAUTH_CLIENT_ID`, OAuth Client Secret = `MCP_OAUTH_CLIENT_SECRET` (both from the service's *Environment* page)

Click *Connect*. A health-cube page asks for a password: paste `MCP_LOGIN_PASSWORD`, keeping it in a password manager. claude.ai stays signed in for up to 30 days of use.

How it is locked down:
- **One client.** claude.ai is the only OAuth client, and dynamic client registration is off, so nobody else can register one.
- **Redirects.** Sign-in can only send you back to claude.ai's callback.
- **Two secrets.** Getting a token needs both the password and the client secret.
- **Wrong passwords.** After 5 wrong passwords in 15 minutes, sign-in locks for 15 minutes.
- **Tokens.** Access tokens last an hour and only work on this server's `/mcp` address. Refresh tokens rotate and can't be reused.
- **Read-only.** The server holds `CUBEJS_API_SECRET`, never the MotherDuck token. Its logs record sign-ins and the queries asked, never the data returned.
- **Sign out everywhere.** Regenerate `MCP_SIGNING_KEY` in Render. That ends every token; reconnect the connector afterwards.

**12. Activity recordings, splits and best efforts.** The `activity_samples` resource loads each activity's per-second recording (time, distance, heart rate, pace, grade-adjusted pace, elevation, cadence, power, running dynamics; GPS is left out). SQLMesh turns runs into `marts.fct_activity_km_splits` (one row per km) and `marts.fct_best_efforts` (fastest 1 km, mile, 5 km, 10 km, half, 30 km and marathon *within* each run), and Garmin's segment totals into `marts.fct_activity_segments`. Cube serves them as the `splits`, `efforts` and `segments` views; VO2max is in `health`.

A run fetches at most a year of recordings (about 2 s each), so the first backfill takes about 18 runs. The incremental models only revisit the last 3 days, so load the history **before** adding the models, or older runs never get splits:
```bash
# from the repo root, not near 06:00 UTC when the Prefect run starts; each pass is up to ~15 min
for i in $(seq 1 18); do WAREHOUSE=motherduck uv run python -m ingest.pipeline garmin || break; done
cd transform && WAREHOUSE=motherduck uv run sqlmesh create_external_models && WAREHOUSE=motherduck uv run sqlmesh plan
```
Render redeploys Cube from the push. The Personal API has no splits, laps, heart-rate zones or weather endpoints, so splits and best efforts are calculated from the recordings; for 2025 onwards they match Garmin's own `fastest_split_*` figures to 0.1 s.

If a recording still fails after the request retries (Garmin sometimes answers 504), that activity is skipped, logged as a warning and fetched again at the start of the next runs, for up to 5 runs. Login and other 4xx errors, or more than 5 failures in one run, still fail the run. The Evidence *Running* page shows personal bests, the best each year, pace against the feels-like temperature, and km splits for the latest 100 runs.

**13. Weather.** `ingest/weather_source.py` loads hourly weather from [Open-Meteo](https://open-meteo.com)'s historical API (free for non-commercial use, no key) for the area each activity started in. Open-Meteo only receives the start point rounded to 0.1° (about 11 km), never the route. `marts.fct_activity_weather` has the temperature, feels-like temperature, humidity and wind for each activity, and Cube adds them to the `training` view. Treadmill and gym sessions have no start point, so no weather. The full history is about 500 calls, well within Open-Meteo's 10,000 a day:
```bash
WAREHOUSE=motherduck uv run python -m ingest.pipeline weather
cd transform && WAREHOUSE=motherduck uv run sqlmesh create_external_models && WAREHOUSE=motherduck uv run sqlmesh plan
```
After that the daily Prefect run fetches weather for new activities, and fetches the last week again because Open-Meteo revises recent days.

**SQL linting.** Two linters, for different things:
- **SQLMesh's linter** checks correctness against the real tables. It runs on every `sqlmesh plan`, and on its own with `cd transform && uv run sqlmesh lint`. Ambiguous or unknown columns, `SELECT *`, unregistered raw tables and unnamed columns stop the plan; a model without audits is a warning. The rules are in `transform/config.py`.
- **SQLFluff** checks style: layout, capitalisation, aliases, join style. Run `uv run python transform/lint_sql.py` to report problems, or add `--fix` to fix what it can. SQLFluff can't parse SQLMesh's `MODEL (...)` header or `@macros`, so the script hides them while SQLFluff runs and puts them back after a fix, so don't run `sqlfluff` directly. The rules are in `.sqlfluff`. Check a fix before committing it: AL05 once deleted an alias that named a `VALUES` list's columns.

Both run before a commit that changes the models, from `.githooks/pre-commit` (SQLFluff on the staged models only). Turn the hook on once per clone with `git config core.hooksPath .githooks`; skip it for one commit with `git commit --no-verify`.

## Notes and limits
- The Garmin Personal API is read-only and covers your own data only. Rate limits aren't published, so the client pauses 0.5s between calls. Max ranges are 1 year (daily summaries), 31 days (sleep) and 28 days (HRV, conservative).
- Every run re-pulls the last 3 days, because Garmin revises recent days. The merge on natural keys keeps rows unique, and SQLMesh `lookback 3` reprocesses the same days.
- SQLMesh state for the motherduck gateway is in Postgres when `SQLMESH_STATE_DB_URL` is set, and in MotherDuck otherwise.
- Withings rotates its refresh token on every use, so the token lives only in the Prefect Secret `withings-refresh-token`. Laptop and cloud runs both read and update it there. Withings asks for at most one poll per user every 10 minutes.
- Cube runs on Render's free tier and sleeps after ~15 minutes idle. Its API needs a JWT signed with `CUBEJS_API_SECRET`, which the MCP server creates per request.
