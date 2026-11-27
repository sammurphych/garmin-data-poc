#!/bin/sh
# Sets up Airflow on a fresh Oracle Linux 9 or Ubuntu server (an Oracle Cloud Always Free VM), from the cloned repo:
#
#   sudo dnf install -y git        # Oracle Linux only; Ubuntu has git
#   git clone https://github.com/sammurphych/garmin-data-poc.git && cd garmin-data-poc
#   (copy the laptop's .env to the repo root first: scp .env <user>@<server>:garmin-data-poc/)
#   The user is opc on Oracle Linux and ubuntu on Ubuntu.
#   airflow/server/install.sh
#
# Safe to run again: it keeps the generated secrets and the database, and rebuilds and restarts the rest.
set -e
cd "$(dirname "$0")"
REPO="$(cd ../.. && pwd)"

[ -f "$REPO/.env" ] || { echo "Copy the pipeline's .env to $REPO/.env first (scp from the laptop)."; exit 1; }
chmod 600 "$REPO/.env"

# A 2 GB swap file, so a memory spike (the first venv build, a big SQLMesh run) slows down instead of crashing.
if ! swapon --show | grep -q /swapfile; then
  sudo fallocate -l 2G /swapfile && sudo chmod 600 /swapfile && sudo mkswap /swapfile && sudo swapon /swapfile
  grep -q /swapfile /etc/fstab || echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab >/dev/null
fi

if ! command -v docker >/dev/null; then
  if command -v dnf >/dev/null; then
    # Oracle Linux: Docker's own script doesn't cover it, but Docker's RHEL packages work on it.
    sudo dnf install -y dnf-plugins-core
    sudo dnf config-manager --add-repo https://download.docker.com/linux/rhel/docker-ce.repo
    sudo dnf install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
    sudo systemctl enable --now docker
  else
    curl -fsSL https://get.docker.com | sudo sh
  fi
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
