# Cube Core on Render (render.yaml) over the SQLMesh marts in MotherDuck.
# Secrets (CUBEJS_DB_DUCKDB_MOTHERDUCK_TOKEN, CUBEJS_API_SECRET) are Render env vars.
FROM cubejs/cube:v1.8.21

COPY model /cube/conf/model

ENV CUBEJS_DB_TYPE=duckdb \
    CUBEJS_DEV_MODE=false \
    CUBEJS_CACHE_AND_QUEUE_DRIVER=memory \
    CUBEJS_TELEMETRY=false \
    PORT=4000
