#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   sudo bash update_nm_bank.sh

APP_DIR="/opt/nm-bank"
SERVICE_NAME="nm-bank.service"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run as root: sudo bash update_nm_bank.sh"
  exit 1
fi

if [[ ! -d "${APP_DIR}/.git" ]]; then
  echo "Repository not found at ${APP_DIR}. Run install script first."
  exit 1
fi

echo "[1/4] Pulling latest code..."
git -C "${APP_DIR}" fetch --all --prune
git -C "${APP_DIR}" reset --hard origin/main

echo "[2/4] Installing updated dependencies..."
"${APP_DIR}/.venv/bin/pip" install -r "${APP_DIR}/requirements.txt"

echo "[3/4] Restarting service..."
systemctl restart "${SERVICE_NAME}"

echo "[4/4] Service summary:"
systemctl --no-pager --full status "${SERVICE_NAME}" | sed -n '1,12p'
echo "Update complete."
