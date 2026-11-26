#!/bin/sh
# Sets up Airflow on a fresh Ubuntu server (an Oracle Cloud Always Free VM), from the cloned repo:
#
#   git clone https://github.com/sammurphych/garmin-data-poc.git && cd garmin-data-poc
#   (copy the laptop's .env to the repo root first: scp .env ubuntu@<server>:garmin-data-poc/)
#   airflow/server/install.sh
#
# Safe to run again: it keeps the generated secrets and the database, and rebuilds and restarts the rest.
set -e
cd "$(dirname "$0")"
REPO="$(cd ../.. && pwd)"

[ -f "$REPO/.env" ] || { echo "Copy the pipeline's .env to $REPO/.env first (scp from the laptop)."; exit 1; }
chmod 600 "$REPO/.env"

if ! command -v docker >/dev/null; then
  curl -fsSL https://get.docker.com | sudo sh
  sudo usermod -aG docker "$USER"
fi
# The first run adds us to the docker group, which only applies to new logins, so use sudo until then.
DOCKER="docker"; docker info >/dev/null 2>&1 || DOCKER="sudo docker"

# Airflow's own secrets, generated once. Compose reads this .env; the pipeline's is the repo's.
if [ ! -f .env ]; then
  umask 077
  cat > .env <<EOF
AIRFLOW_UID=$(id -u)
POSTGRES_PASSWORD=$(openssl rand -hex 24)
FERNET_KEY=$(openssl rand -base64 32 | tr '+/' '-_')
JWT_SECRET=$(openssl rand -hex 32)
API_SECRET_KEY=$(openssl rand -hex 32)
EOF
  umask 022
fi

mkdir -p state/logs
if [ ! -f state/passwords.json ]; then
  printf '{"admin": "%s"}\n' "$(openssl rand -hex 12)" > state/passwords.json
  chmod 600 state/passwords.json
fi

$DOCKER compose up -d --build
echo
echo "Airflow is starting (the first start builds the pipeline's venv, a few minutes)."
echo "From the laptop:  ssh -L 8080:localhost:8080 $USER@<this server>   then open http://localhost:8080"
echo "Login: admin / $(sed 's/.*: "\(.*\)".*/\1/' state/passwords.json)"
