# Cube Core on Render (render.yaml) over the SQLMesh marts in MotherDuck.
# Secrets (CUBEJS_DB_DUCKDB_MOTHERDUCK_TOKEN, CUBEJS_API_SECRET) are Render env vars.
FROM cubejs/cube:v1.8.21

# The MotherDuck extension talks gRPC over TLS and looks for root certificates at
# /usr/share/grpc/roots.pem, which this image lacks ("Could not get default pem root
# certs", then "Could not connect to MotherDuck ... UNAVAILABLE"). Install the system
# CA bundle and point gRPC at it.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
ENV GRPC_DEFAULT_SSL_ROOTS_FILE_PATH=/etc/ssl/certs/ca-certificates.crt

COPY model /cube/conf/model

ENV CUBEJS_DB_TYPE=duckdb \
    CUBEJS_DEV_MODE=false \
    CUBEJS_CACHE_AND_QUEUE_DRIVER=memory \
    CUBEJS_TELEMETRY=false \
    PORT=4000
