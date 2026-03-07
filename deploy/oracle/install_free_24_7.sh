#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   sudo bash install_free_24_7.sh [REPO_URL]
# Example:
#   sudo bash install_free_24_7.sh https://github.com/demon140582/nm-bank.git

REPO_URL="${1:-https://github.com/demon140582/nm-bank.git}"
APP_USER="nmbank"
APP_DIR="/opt/nm-bank"
DATA_DIR="/var/lib/nm-bank"
ENV_DIR="/etc/nm-bank"
ENV_FILE="${ENV_DIR}/nm-bank.env"
SERVICE_FILE="/etc/systemd/system/nm-bank.service"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run as root: sudo bash install_free_24_7.sh [REPO_URL]"
  exit 1
fi

echo "[1/8] Installing base packages..."
apt-get update -y
DEBIAN_FRONTEND=noninteractive apt-get install -y git python3 python3-venv python3-pip curl

echo "[2/8] Creating service user..."
if ! id -u "${APP_USER}" >/dev/null 2>&1; then
  useradd --system --create-home --home-dir "/home/${APP_USER}" --shell /usr/sbin/nologin "${APP_USER}"
fi

echo "[3/8] Cloning/updating repository..."
if [[ -d "${APP_DIR}/.git" ]]; then
  git -C "${APP_DIR}" fetch --all --prune
  git -C "${APP_DIR}" reset --hard origin/main
else
  rm -rf "${APP_DIR}"
  git clone "${REPO_URL}" "${APP_DIR}"
fi

echo "[4/8] Creating virtual environment and installing dependencies..."
python3 -m venv "${APP_DIR}/.venv"
"${APP_DIR}/.venv/bin/pip" install --upgrade pip
"${APP_DIR}/.venv/bin/pip" install -r "${APP_DIR}/requirements.txt"

echo "[5/8] Preparing persistent data and environment..."
mkdir -p "${DATA_DIR}" "${ENV_DIR}"
chown -R "${APP_USER}:${APP_USER}" "${APP_DIR}" "${DATA_DIR}"
chmod 750 "${DATA_DIR}"

if [[ ! -f "${ENV_FILE}" ]]; then
  SECRET="$("${APP_DIR}/.venv/bin/python" -c "import secrets; print(secrets.token_hex(32))")"
  cat > "${ENV_FILE}" <<EOF
NM_BANK_SECRET=${SECRET}
NM_BANK_DB_PATH=${DATA_DIR}/bank.db
PORT=5000
EOF
fi
chown root:"${APP_USER}" "${ENV_FILE}"
chmod 640 "${ENV_FILE}"

echo "[6/8] Writing systemd service..."
cat > "${SERVICE_FILE}" <<'EOF'
[Unit]
Description=NM-Bank Flask Service
After=network.target

[Service]
Type=simple
User=nmbank
Group=nmbank
WorkingDirectory=/opt/nm-bank
EnvironmentFile=/etc/nm-bank/nm-bank.env
ExecStart=/opt/nm-bank/.venv/bin/gunicorn --workers 1 --threads 8 --bind 0.0.0.0:5000 server:app
Restart=always
RestartSec=5
KillMode=mixed
TimeoutStopSec=20

[Install]
WantedBy=multi-user.target
EOF

echo "[7/8] Enabling and starting service..."
systemctl daemon-reload
systemctl enable --now nm-bank.service

echo "[8/8] Opening firewall (if ufw exists)..."
if command -v ufw >/dev/null 2>&1; then
  ufw allow 22/tcp || true
  ufw allow 5000/tcp || true
fi

PUBLIC_IP="$(curl -fsSL https://api.ipify.org || true)"
if [[ -z "${PUBLIC_IP}" ]]; then
  PUBLIC_IP="$(hostname -I | awk '{print $1}')"
fi

echo
echo "NM-Bank installed successfully."
echo "Service status:"
systemctl --no-pager --full status nm-bank.service | sed -n '1,12p'
echo
echo "Health check URL: http://${PUBLIC_IP}:5000/healthz"
echo "App URL:          http://${PUBLIC_IP}:5000/"
