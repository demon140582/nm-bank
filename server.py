import hashlib
import base64
import binascii
import json
import os
import random
import re
import secrets
import shutil
import socket
import sqlite3
import string
import subprocess
import threading
import time
import urllib.request
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from flask import Flask, g, jsonify, request, send_from_directory, session


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("NM_BANK_DB_PATH", os.path.join(BASE_DIR, "bank.db"))
DB_DIR = os.path.dirname(DB_PATH)
if DB_DIR and not os.path.isdir(DB_DIR):
    os.makedirs(DB_DIR, exist_ok=True)

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("NM_BANK_SECRET", secrets.token_hex(32))
app.config["JSON_AS_ASCII"] = False


@app.after_request
def apply_no_cache_headers(response):
    path = str(request.path or "")
    if path == "/" or path.endswith(".html") or path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


@app.get("/healthz")
def health_check():
    return jsonify({"ok": True, "status": "healthy", "service": "NM-Bank"})


ADMIN_PASSWORD = "123123admin123123"
CLICK_COOLDOWN_SECONDS = 1.0
GAME_COOLDOWN_SECONDS = 2.0
ANTI_ABUSE_STATE: Dict[int, Dict[str, float]] = {}
NGROK_PORT = int(os.environ.get("NM_BANK_TUNNEL_PORT", "5000"))
CLOUDFLARE_HOST_HINT = "trycloudflare.com"
LOCALHOST_RUN_HOST = "localhost.run"
PINGGY_HOST_HINT = "pinggy.link"
PINGGY_HOST = "a.pinggy.io"
NGROK_LOCK = threading.RLock()
NGROK_LOG_LOCK = threading.RLock()
NGROK_PROCESS: Optional[subprocess.Popen] = None
NGROK_PUBLIC_URL: Optional[str] = None
NGROK_LAST_ERROR: Optional[str] = None
NGROK_STARTED_AT: Optional[str] = None
NGROK_PROVIDER: str = "cloudflare_quick_tunnel"
NGROK_PREFERRED_PROVIDER: str = str(os.environ.get("NM_BANK_TUNNEL_PROVIDER", "auto") or "auto").strip().lower()
NGROK_LOG_LINES: List[str] = []
NGROK_READER_THREAD: Optional[threading.Thread] = None

VIP_CONFIG = {
    0: {"name": "Обычный", "icon": "🙂", "color": "#b8c2d6", "win_chance": 43},
    1: {"name": "Bronze", "icon": "🟤", "color": "#cd7f32", "win_chance": 48},
    2: {"name": "Silver", "icon": "⚪", "color": "#c0c0c0", "win_chance": 53},
    3: {"name": "Gold", "icon": "🟡", "color": "#ffd700", "win_chance": 58},
    4: {"name": "Diamond", "icon": "🔷", "color": "#59bfff", "win_chance": 63},
}

VIP_UPGRADE_COSTS = {1: 7000, 2: 22000, 3: 60000, 4: 140000}
CARD_PREFIX = "2200 7012"
CARD_LIMITS_BY_VIP = {0: 2, 1: 3, 2: 5, 3: 7, 4: 10}
DEFAULT_USD_RATE = Decimal("92.50")
USD_RATE_MIN = Decimal("40.00")
USD_RATE_MAX = Decimal("220.00")
USD_RATE_AUTO_SECONDS = 40
USD_RATE_MAX_SHIFT_PERCENT = Decimal("1.80")
MAX_AVATAR_BYTES = 1_500_000
MAX_AVATAR_DATA_URL_LENGTH = 2_200_000
ROOT_USERNAME = "d1maxturk0v"
ADMIN_LEVEL_NAMES = {
    0: "Пользователь",
    1: "Тестовый",
    2: "Служба поддержки",
    3: "Модератор",
    4: "Старший модератор",
    5: "Администратор",
    6: "Лидер",
    7: "Создатель",
}
MAX_BAN_DAYS_BY_ADMIN_LEVEL = {0: 0, 1: 0, 2: 0, 3: 10, 4: 20, 5: 30, 6: 30, 7: 365000}
MAX_GRANT_ADMIN_LEVEL = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 6, 7: 7}
ACHIEVEMENT_LABELS = {
    "first_click": "Первый клик",
    "first_game": "Первая игра",
    "first_win": "Первая победа",
    "first_transfer": "Первый перевод",
    "vip_purchase": "Покупка VIP",
}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_json(value: Optional[str], fallback: Any) -> Any:
    if not value:
        return fallback
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return fallback


def to_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def is_root_user(user_row) -> bool:
    if not user_row:
        return False
    return str(user_row["username"]).strip().lower() == ROOT_USERNAME


def is_admin_user(user_row) -> bool:
    return bool(user_row and (int(user_row["admin_level"]) > 0 or is_root_user(user_row)))


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=15)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=15000")
        g.db = conn
    return g.db


@app.teardown_appcontext
def close_db(_error: Optional[Exception]):
    conn = g.pop("db", None)
    if conn is not None:
        conn.close()


def init_db() -> None:
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.execute("PRAGMA foreign_keys=ON")
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            balance INTEGER NOT NULL DEFAULT 0,
            vip_level INTEGER NOT NULL DEFAULT 0,
            level INTEGER NOT NULL DEFAULT 1,
            achievements TEXT NOT NULL DEFAULT '[]',
            ref_code TEXT UNIQUE NOT NULL,
            ref_by INTEGER,
            referral_earnings INTEGER NOT NULL DEFAULT 0,
            last_daily TEXT,
            last_active TEXT,
            suspicious INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (ref_by) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            amount INTEGER NOT NULL,
            total_to_pay INTEGER NOT NULL,
            taken_at TEXT NOT NULL,
            due_at TEXT NOT NULL,
            repaid INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            action TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            info TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS cards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            card_number TEXT UNIQUE NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS support_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            sender_user_id INTEGER NOT NULL,
            sender_role TEXT NOT NULL,
            message TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (sender_user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_chat_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_user_id INTEGER NOT NULL,
            message TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            FOREIGN KEY (sender_user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            title TEXT NOT NULL,
            message TEXT NOT NULL,
            info TEXT,
            timestamp TEXT NOT NULL,
            read INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS public_chat_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_user_id INTEGER NOT NULL,
            message TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            FOREIGN KEY (sender_user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS messenger_private_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_user_id INTEGER NOT NULL,
            receiver_user_id INTEGER NOT NULL,
            message TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            FOREIGN KEY (sender_user_id) REFERENCES users(id),
            FOREIGN KEY (receiver_user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS messenger_channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE NOT NULL,
            title TEXT NOT NULL,
            owner_user_id INTEGER NOT NULL,
            is_public INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            FOREIGN KEY (owner_user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS messenger_channel_members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            joined_at TEXT NOT NULL,
            UNIQUE(channel_id, user_id),
            FOREIGN KEY (channel_id) REFERENCES messenger_channels(id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS messenger_channel_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            sender_user_id INTEGER NOT NULL,
            message TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            FOREIGN KEY (channel_id) REFERENCES messenger_channels(id),
            FOREIGN KEY (sender_user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS messenger_call_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            caller_user_id INTEGER NOT NULL,
            target_user_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            FOREIGN KEY (caller_user_id) REFERENCES users(id),
            FOREIGN KEY (target_user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS economy_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    # Lightweight migration for older DBs.
    user_columns = {row[1] for row in cur.execute("PRAGMA table_info(users)").fetchall()}
    if "full_name" not in user_columns:
        cur.execute("ALTER TABLE users ADD COLUMN full_name TEXT NOT NULL DEFAULT ''")
    if "admin_level" not in user_columns:
        cur.execute("ALTER TABLE users ADD COLUMN admin_level INTEGER NOT NULL DEFAULT 0")
    if "warnings" not in user_columns:
        cur.execute("ALTER TABLE users ADD COLUMN warnings INTEGER NOT NULL DEFAULT 0")
    if "banned_until" not in user_columns:
        cur.execute("ALTER TABLE users ADD COLUMN banned_until TEXT")
    if "support_last_seen_user" not in user_columns:
        cur.execute("ALTER TABLE users ADD COLUMN support_last_seen_user TEXT")
    if "support_last_seen_admin" not in user_columns:
        cur.execute("ALTER TABLE users ADD COLUMN support_last_seen_admin TEXT")
    if "admin_chat_last_seen" not in user_columns:
        cur.execute("ALTER TABLE users ADD COLUMN admin_chat_last_seen TEXT")
    if "admin_panel_password_hash" not in user_columns:
        cur.execute("ALTER TABLE users ADD COLUMN admin_panel_password_hash TEXT")
    if "public_chat_last_seen" not in user_columns:
        cur.execute("ALTER TABLE users ADD COLUMN public_chat_last_seen TEXT")
    if "balance_usd" not in user_columns:
        cur.execute("ALTER TABLE users ADD COLUMN balance_usd REAL NOT NULL DEFAULT 0")
    if "avatar_url" not in user_columns:
        cur.execute("ALTER TABLE users ADD COLUMN avatar_url TEXT NOT NULL DEFAULT ''")

    cur.execute("UPDATE users SET full_name = username WHERE full_name IS NULL OR full_name = ''")
    cur.execute("UPDATE users SET admin_level = 6 WHERE lower(username) = 'admin' AND admin_level < 6")
    cur.execute("UPDATE users SET admin_level = 7 WHERE lower(username) = ? AND admin_level < 7", (ROOT_USERNAME,))

    now_iso = to_iso(now_utc())
    cur.execute(
        "INSERT OR IGNORE INTO economy_settings (key, value, updated_at) VALUES ('usd_rate', ?, ?)",
        (str(DEFAULT_USD_RATE), now_iso),
    )
    cur.execute(
        "INSERT OR IGNORE INTO economy_settings (key, value, updated_at) VALUES ('card_limits_by_vip', ?, ?)",
        (json.dumps({str(k): int(v) for k, v in CARD_LIMITS_BY_VIP.items()}), now_iso),
    )

    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_ref_code ON users(ref_code)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_last_active ON users(last_active)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_credits_user_repaid ON credits(user_id, repaid)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_logs_user_timestamp ON logs(user_id, timestamp)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cards_user ON cards(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_support_user ON support_messages(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_support_timestamp ON support_messages(timestamp)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_admin_chat_timestamp ON admin_chat_messages(timestamp)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_notifications_user_read_time ON notifications(user_id, read, timestamp)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_public_chat_timestamp ON public_chat_messages(timestamp)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messenger_pm_pair ON messenger_private_messages(sender_user_id, receiver_user_id, id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messenger_channels_owner ON messenger_channels(owner_user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messenger_channel_members_user ON messenger_channel_members(user_id, channel_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messenger_channel_messages_channel ON messenger_channel_messages(channel_id, id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messenger_calls_users ON messenger_call_events(caller_user_id, target_user_id, id)")
    conn.commit()
    conn.close()


# Ensure schema and migrations are applied for any run mode (python server.py, flask run, imports).
init_db()


def normalize_tunnel_provider(value: Optional[str]) -> str:
    raw = str(value or "auto").strip().lower()
    aliases = {
        "auto": "auto",
        "cloudflare": "cloudflare_quick_tunnel",
        "cloudflare_quick_tunnel": "cloudflare_quick_tunnel",
        "cf": "cloudflare_quick_tunnel",
        "pinggy": "pinggy",
        "localhost": "localhost_run",
        "localhost.run": "localhost_run",
        "localhost_run": "localhost_run",
    }
    return aliases.get(raw, "auto")


def tunnel_provider_candidates(preferred: str) -> List[str]:
    normalized = normalize_tunnel_provider(preferred)
    if normalized == "auto":
        return ["cloudflare_quick_tunnel", "pinggy", "localhost_run"]
    return [normalized]


def tunnel_provider_options(cloudflared_exe: Optional[str], ssh_exe: Optional[str]) -> List[Dict[str, Any]]:
    return [
        {
            "key": "auto",
            "label": "Авто (Cloudflare -> Pinggy -> localhost.run)",
            "available": bool(cloudflared_exe or ssh_exe),
        },
        {
            "key": "cloudflare_quick_tunnel",
            "label": "Cloudflare Quick Tunnel",
            "available": bool(cloudflared_exe),
        },
        {
            "key": "pinggy",
            "label": "Pinggy SSH Tunnel",
            "available": bool(ssh_exe),
        },
        {
            "key": "localhost_run",
            "label": "localhost.run SSH Tunnel",
            "available": bool(ssh_exe),
        },
    ]


def find_ngrok_executable() -> Optional[str]:
    env_path = os.environ.get("CLOUDFLARED_PATH", "").strip() or os.environ.get("CF_PATH", "").strip()
    if env_path and os.path.isfile(env_path):
        return env_path
    found = shutil.which("cloudflared")
    if found:
        return found
    user_profile = os.environ.get("USERPROFILE", "")
    if os.name == "nt":
        common_paths = [
            os.path.join(BASE_DIR, "cloudflared.exe"),
            os.path.join(user_profile, "cloudflared.exe"),
            os.path.join(user_profile, "AppData", "Local", "cloudflared", "cloudflared.exe"),
            os.path.join("C:\\", "Program Files", "cloudflared", "cloudflared.exe"),
        ]
    else:
        common_paths = [
            os.path.join(BASE_DIR, "cloudflared"),
            "/usr/local/bin/cloudflared",
            "/usr/bin/cloudflared",
        ]
    for path in common_paths:
        if path and os.path.isfile(path):
            return path
    return None


def _find_ssh_executable() -> Optional[str]:
    env_path = os.environ.get("SSH_PATH", "").strip()
    if env_path and os.path.isfile(env_path):
        return env_path
    found = shutil.which("ssh")
    if found:
        return found
    windir = os.environ.get("WINDIR", "C:\\Windows")
    common_paths = [
        os.path.join(windir, "System32", "OpenSSH", "ssh.exe"),
        os.path.join(BASE_DIR, "ssh.exe"),
    ]
    for path in common_paths:
        if path and os.path.isfile(path):
            return path
    return None


def _cloudflared_download_url() -> Optional[str]:
    platform_name = os.environ.get("OS", "").lower()
    machine = os.environ.get("PROCESSOR_ARCHITECTURE", "").lower()
    if os.name != "nt":
        platform_name = os.uname().sysname.lower() if hasattr(os, "uname") else platform_name
        machine = os.uname().machine.lower() if hasattr(os, "uname") else machine

    if "windows" in platform_name or os.name == "nt":
        if "arm64" in machine:
            return "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-arm64.exe"
        return "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe"
    if "linux" in platform_name:
        if "arm64" in machine or "aarch64" in machine:
            return "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64"
        return "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64"
    return None


def _cloudflared_target_path() -> str:
    if os.name == "nt":
        return os.path.join(BASE_DIR, "cloudflared.exe")
    return os.path.join(BASE_DIR, "cloudflared")


def _download_cloudflared_binary() -> Optional[str]:
    url = _cloudflared_download_url()
    if not url:
        with NGROK_LOCK:
            NGROK_LAST_ERROR = "Автозагрузка cloudflared не поддерживается на этой ОС."
        return None

    target = _cloudflared_target_path()
    try:
        with urllib.request.urlopen(url, timeout=60) as response:
            data = response.read()
        if not data:
            raise OSError("Пустой ответ от сервера")
        with open(target, "wb") as f:
            f.write(data)
        if os.name != "nt":
            current_mode = os.stat(target).st_mode
            os.chmod(target, current_mode | 0o111)
        return target
    except Exception as exc:
        with NGROK_LOCK:
            NGROK_LAST_ERROR = f"Не удалось скачать cloudflared: {exc}"
        try:
            if os.path.isfile(target):
                os.remove(target)
        except OSError:
            pass
        return None


def _extract_public_url_from_line(line: str) -> Optional[str]:
    if not line:
        return None
    clean_line = re.sub(r"\x1b\[[0-9;]*m", "", str(line or ""))
    lowered = clean_line.lower()

    # Some SSH tunnel providers print host without scheme.
    host_hint_match = re.search(r"\b([a-z0-9.-]+\.(?:localhost\.run|lhr\.life|pinggy\.link))\b", lowered)
    if host_hint_match and "tunneled" in lowered:
        return f"https://{host_hint_match.group(1)}"

    candidates = re.findall(r"https?://[a-zA-Z0-9._:/-]+", clean_line)
    best_url = None
    best_score = -10_000
    for raw in candidates:
        url = str(raw).rstrip(".,;")
        try:
            parsed = urlparse(url)
            host = (parsed.hostname or "").lower()
            path = str(parsed.path or "")
        except ValueError:
            host = ""
            path = ""
        if not host or host in {"127.0.0.1", "localhost"}:
            continue

        score = 0
        if CLOUDFLARE_HOST_HINT in host or "cfargotunnel.com" in host:
            score += 120
        if host.endswith(".localhost.run") or host.endswith(".lhr.life"):
            score += 100
        if host.endswith(".pinggy.link"):
            score += 105
        if "tunneled" in lowered:
            score += 20
        if host.startswith("admin.localhost.run"):
            score -= 150
        if "/docs" in path:
            score -= 120
        if path and path not in {"", "/"}:
            score -= 20

        if score > best_score:
            best_score = score
            best_url = url

    if best_url:
        return best_url
    return None


def _url_quality(url: Optional[str]) -> int:
    if not url:
        return -10_000
    try:
        parsed = urlparse(str(url))
        host = (parsed.hostname or "").lower()
        path = str(parsed.path or "")
    except ValueError:
        return -10_000
    score = 0
    if CLOUDFLARE_HOST_HINT in host or "cfargotunnel.com" in host:
        score += 120
    if host.endswith(".localhost.run") or host.endswith(".lhr.life"):
        score += 100
    if host.endswith(".pinggy.link"):
        score += 105
    if host in {"localhost.run", "admin.localhost.run"}:
        score -= 120
    if "/docs" in path:
        score -= 120
    if path and path not in {"", "/"}:
        score -= 20
    return score


def _append_tunnel_log_line(line: str) -> None:
    global NGROK_PUBLIC_URL, NGROK_LAST_ERROR
    clean = str(line or "").strip()
    if not clean:
        return
    with NGROK_LOG_LOCK:
        NGROK_LOG_LINES.append(clean)
        if len(NGROK_LOG_LINES) > 80:
            del NGROK_LOG_LINES[:-80]

    parsed_url = _extract_public_url_from_line(clean)
    if parsed_url:
        with NGROK_LOCK:
            parsed_quality = _url_quality(parsed_url)
            current_quality = _url_quality(NGROK_PUBLIC_URL)
            if parsed_quality >= 0 and parsed_quality >= current_quality:
                NGROK_PUBLIC_URL = parsed_url
                NGROK_LAST_ERROR = None

    lowered = clean.lower()
    if any(
        token in lowered
        for token in (
            "remote port forwarding failed",
            "administratively prohibited",
            "permission denied",
            "could not resolve hostname",
            "connection refused",
            "connection reset",
            "kex_exchange_identification",
            "error",
            "failed",
        )
    ):
        with NGROK_LOCK:
            NGROK_LAST_ERROR = clean


def _tunnel_reader(process: subprocess.Popen) -> None:
    stream = process.stdout
    if stream is None:
        return
    try:
        for raw_line in iter(stream.readline, ""):
            if raw_line == "":
                break
            _append_tunnel_log_line(raw_line)
    except Exception:
        pass
    finally:
        try:
            stream.close()
        except Exception:
            pass


def _tunnel_log_tail(lines: int = 5) -> str:
    with NGROK_LOG_LOCK:
        if not NGROK_LOG_LINES:
            return ""
        part = NGROK_LOG_LINES[-max(1, lines) :]
    return " | ".join(part)


def get_ngrok_status() -> Dict[str, Any]:
    global NGROK_PROCESS, NGROK_PUBLIC_URL, NGROK_LAST_ERROR, NGROK_STARTED_AT, NGROK_PROVIDER, NGROK_PREFERRED_PROVIDER
    with NGROK_LOCK:
        cloudflared_exe = find_ngrok_executable()
        ssh_exe = _find_ssh_executable()
        preferred_provider = normalize_tunnel_provider(NGROK_PREFERRED_PROVIDER)
        provider_options = tunnel_provider_options(cloudflared_exe, ssh_exe)
        provider_available = {
            "cloudflare_quick_tunnel": bool(cloudflared_exe),
            "pinggy": bool(ssh_exe),
            "localhost_run": bool(ssh_exe),
        }
        selected_available = bool(cloudflared_exe or ssh_exe) if preferred_provider == "auto" else bool(provider_available.get(preferred_provider))
        if preferred_provider == "cloudflare_quick_tunnel":
            selected_executable = cloudflared_exe
        elif preferred_provider in {"pinggy", "localhost_run"}:
            selected_executable = ssh_exe
        else:
            selected_executable = cloudflared_exe or ssh_exe

        running = False
        pid = None
        if NGROK_PROCESS is not None:
            code = NGROK_PROCESS.poll()
            if code is None:
                running = True
                pid = NGROK_PROCESS.pid
            else:
                NGROK_PROCESS = None
                NGROK_PUBLIC_URL = None
                NGROK_STARTED_AT = None
                if not NGROK_LAST_ERROR:
                    tail = _tunnel_log_tail()
                    NGROK_LAST_ERROR = f"Туннель завершился (код {code}).{(' ' + tail) if tail else ''}"

        public_url = NGROK_PUBLIC_URL if running else None
        host = None
        ip = None
        if public_url:
            try:
                host = urlparse(public_url).hostname
            except ValueError:
                host = None
            host_lc = str(host or "").lower()
            if host_lc and (("ngrok" in host_lc) or host_lc.endswith("serveo.net")):
                # Legacy URL guard: old providers are no longer used in runtime tunnel manager.
                NGROK_PUBLIC_URL = None
                public_url = None
                host = None
                NGROK_LAST_ERROR = "Обнаружена устаревшая ссылка старого туннеля. Обновите статус или перезапустите сервер."
            if host:
                try:
                    ip = socket.gethostbyname(host)
                except OSError:
                    ip = None
        status_error = NGROK_LAST_ERROR
        if running and public_url and status_error:
            lowered = str(status_error).lower()
            transient_tokens = (
                "quic",
                "tls handshake",
                "retrying connection",
                "serve tunnel error",
                "timeout: no recent network activity",
            )
            if any(token in lowered for token in transient_tokens):
                status_error = None

        return {
            "provider": NGROK_PROVIDER,
            "preferred_provider": preferred_provider,
            "provider_options": provider_options,
            "available": selected_available,
            "executable": selected_executable,
            "cloudflared_available": bool(cloudflared_exe),
            "ssh_available": bool(ssh_exe),
            "running": running,
            "pid": pid,
            "public_url": public_url,
            "public_host": host,
            "public_ip": ip,
            "started_at": NGROK_STARTED_AT if running else None,
            "error": status_error,
        }


def start_ngrok_tunnel() -> Dict[str, Any]:
    global NGROK_PROCESS, NGROK_PUBLIC_URL, NGROK_LAST_ERROR, NGROK_STARTED_AT, NGROK_READER_THREAD, NGROK_PROVIDER, NGROK_PREFERRED_PROVIDER
    with NGROK_LOCK:
        cloudflared_exe = find_ngrok_executable()
        if not cloudflared_exe:
            cloudflared_exe = _download_cloudflared_binary()

        if NGROK_PROCESS is not None and NGROK_PROCESS.poll() is None:
            return get_ngrok_status()

        ssh_exe = _find_ssh_executable()

        def build_tunnel_cmd(provider_key: str) -> Optional[List[str]]:
            if provider_key == "cloudflare_quick_tunnel" and cloudflared_exe:
                return [
                    cloudflared_exe,
                    "tunnel",
                    "--url",
                    f"http://127.0.0.1:{NGROK_PORT}",
                    "--protocol",
                    "http2",
                    "--no-autoupdate",
                ]
            if provider_key == "pinggy" and ssh_exe:
                return [
                    ssh_exe,
                    "-T",
                    "-p",
                    "443",
                    "-o",
                    "StrictHostKeyChecking=no",
                    "-o",
                    "ExitOnForwardFailure=yes",
                    "-o",
                    "ServerAliveInterval=30",
                    "-o",
                    "ServerAliveCountMax=3",
                    "-R",
                    f"0:127.0.0.1:{NGROK_PORT}",
                    f"qr@{PINGGY_HOST}",
                ]
            if provider_key == "localhost_run" and ssh_exe:
                return [
                    ssh_exe,
                    "-T",
                    "-o",
                    "StrictHostKeyChecking=no",
                    "-o",
                    "ExitOnForwardFailure=yes",
                    "-o",
                    "ServerAliveInterval=30",
                    "-o",
                    "ServerAliveCountMax=3",
                    "-R",
                    f"80:127.0.0.1:{NGROK_PORT}",
                    f"nokey@{LOCALHOST_RUN_HOST}",
                ]
            return None

        cmd: Optional[List[str]] = None
        selected = normalize_tunnel_provider(NGROK_PREFERRED_PROVIDER)
        for candidate in tunnel_provider_candidates(selected):
            built = build_tunnel_cmd(candidate)
            if built:
                NGROK_PROVIDER = candidate
                cmd = built
                break

        if not cmd:
            if selected == "cloudflare_quick_tunnel":
                NGROK_LAST_ERROR = "Выбран Cloudflare, но cloudflared не найден."
            elif selected == "pinggy":
                NGROK_LAST_ERROR = "Выбран Pinggy, но ssh-клиент не найден."
            elif selected == "localhost_run":
                NGROK_LAST_ERROR = "Выбран localhost.run, но ssh-клиент не найден."
            else:
                NGROK_LAST_ERROR = "Не удалось подготовить туннель: нет cloudflared и ssh-клиента."
            return get_ngrok_status()

        NGROK_PUBLIC_URL = None
        NGROK_LAST_ERROR = None
        NGROK_STARTED_AT = None
        with NGROK_LOG_LOCK:
            NGROK_LOG_LINES.clear()
        try:
            NGROK_PROCESS = subprocess.Popen(
                cmd,
                cwd=BASE_DIR,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="ignore",
                bufsize=1,
            )
        except OSError as exc:
            NGROK_PROCESS = None
            NGROK_LAST_ERROR = f"Не удалось запустить туннель: {exc}"
            return get_ngrok_status()
        NGROK_STARTED_AT = to_iso(now_utc())
        NGROK_READER_THREAD = threading.Thread(target=_tunnel_reader, args=(NGROK_PROCESS,), daemon=True)
        NGROK_READER_THREAD.start()

    for _ in range(40):
        time.sleep(0.25)
        status = get_ngrok_status()
        if not status["running"]:
            return status
        if status["public_url"]:
            break

    fallback_to_localhost = False
    fallback_reason = ""
    with NGROK_LOCK:
        if NGROK_PROCESS is None:
            NGROK_LAST_ERROR = NGROK_LAST_ERROR or "Туннель не запущен."
            return get_ngrok_status()
        if NGROK_PROCESS.poll() is not None:
            code = NGROK_PROCESS.poll()
            NGROK_PROCESS = None
            NGROK_PUBLIC_URL = None
            NGROK_STARTED_AT = None
            tail = _tunnel_log_tail()
            NGROK_LAST_ERROR = f"Туннель завершился сразу после запуска (код {code}).{(' ' + tail) if tail else ''}"
            return get_ngrok_status()
        if NGROK_PUBLIC_URL:
            NGROK_LAST_ERROR = None
        else:
            fallback_to_localhost = bool(ssh_exe and NGROK_PROVIDER != "localhost_run")
            if fallback_to_localhost:
                fallback_reason = f"Провайдер {NGROK_PROVIDER} не дал публичную ссылку, выполнен авто-фолбэк на localhost.run."
            else:
                NGROK_LAST_ERROR = "Tunnel запущен, но публичная ссылка ещё не получена. Подождите 2-5 секунд и обновите статус."

    if fallback_to_localhost:
        stop_ngrok_tunnel()
        with NGROK_LOCK:
            fallback_cmd = build_tunnel_cmd("localhost_run")
            if not fallback_cmd:
                NGROK_LAST_ERROR = fallback_reason
                return get_ngrok_status()
            NGROK_PROVIDER = "localhost_run"
            NGROK_PUBLIC_URL = None
            NGROK_LAST_ERROR = None
            NGROK_STARTED_AT = None
            with NGROK_LOG_LOCK:
                NGROK_LOG_LINES.clear()
            try:
                NGROK_PROCESS = subprocess.Popen(
                    fallback_cmd,
                    cwd=BASE_DIR,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="ignore",
                    bufsize=1,
                )
            except OSError as exc:
                NGROK_PROCESS = None
                NGROK_LAST_ERROR = f"{fallback_reason} Не удалось запустить localhost.run: {exc}"
                return get_ngrok_status()
            NGROK_STARTED_AT = to_iso(now_utc())
            NGROK_READER_THREAD = threading.Thread(target=_tunnel_reader, args=(NGROK_PROCESS,), daemon=True)
            NGROK_READER_THREAD.start()
        for _ in range(40):
            time.sleep(0.25)
            status = get_ngrok_status()
            if not status["running"]:
                break
            if status["public_url"]:
                with NGROK_LOCK:
                    NGROK_LAST_ERROR = fallback_reason
                break
    return get_ngrok_status()


def stop_ngrok_tunnel() -> Dict[str, Any]:
    global NGROK_PROCESS, NGROK_PUBLIC_URL, NGROK_LAST_ERROR, NGROK_STARTED_AT
    with NGROK_LOCK:
        process = NGROK_PROCESS
        NGROK_PROCESS = None
        NGROK_PUBLIC_URL = None
        NGROK_STARTED_AT = None
        NGROK_LAST_ERROR = None
        with NGROK_LOG_LOCK:
            NGROK_LOG_LINES.clear()

    if process and process.poll() is None:
        try:
            process.terminate()
            process.wait(timeout=3)
        except OSError:
            pass
        except subprocess.TimeoutExpired:
            try:
                process.kill()
            except OSError:
                pass

    return get_ngrok_status()


def log_action(user_id: Optional[int], action: str, info: Optional[Dict[str, Any]] = None) -> None:
    db = get_db()
    db.execute(
        "INSERT INTO logs (user_id, action, timestamp, info) VALUES (?, ?, ?, ?)",
        (user_id, action, to_iso(now_utc()), json.dumps(info or {}, ensure_ascii=False)),
    )
    db.commit()


def add_notification(user_id: int, kind: str, title: str, message: str, info: Optional[Dict[str, Any]] = None) -> None:
    db = get_db()
    db.execute(
        """
        INSERT INTO notifications (user_id, kind, title, message, info, timestamp, read)
        VALUES (?, ?, ?, ?, ?, ?, 0)
        """,
        (int(user_id), kind, title, message, json.dumps(info or {}, ensure_ascii=False), to_iso(now_utc())),
    )
    db.commit()


def get_notifications_unread_count(user_id: int) -> int:
    row = get_db().execute(
        "SELECT COUNT(*) AS cnt FROM notifications WHERE user_id = ? AND read = 0",
        (int(user_id),),
    ).fetchone()
    return int(row["cnt"]) if row else 0


def get_public_chat_unread_count(user_id: int) -> int:
    user = get_user_by_id(int(user_id))
    if not user:
        return 0
    last_seen = user["public_chat_last_seen"] if "public_chat_last_seen" in user.keys() else None
    last_seen = last_seen or "1970-01-01T00:00:00+00:00"
    row = get_db().execute(
        """
        SELECT COUNT(*) AS cnt
        FROM public_chat_messages
        WHERE sender_user_id != ? AND timestamp > ?
        """,
        (int(user_id), last_seen),
    ).fetchone()
    return int(row["cnt"]) if row else 0


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def create_ref_code() -> str:
    db = get_db()
    alphabet = string.ascii_uppercase + string.digits
    for _ in range(50):
        code = "".join(random.choice(alphabet) for _ in range(8))
        if not db.execute("SELECT id FROM users WHERE ref_code = ?", (code,)).fetchone():
            return code
    return secrets.token_hex(4).upper()


def get_user_by_id(user_id: int):
    return get_db().execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()


def get_user_by_username(username: str):
    return get_db().execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()


def update_last_active(user_id: int) -> None:
    db = get_db()
    db.execute("UPDATE users SET last_active = ? WHERE id = ?", (to_iso(now_utc()), user_id))
    db.commit()


def is_online_by_last_active(last_active_raw: Optional[str], within_seconds: int = 60) -> bool:
    active = parse_iso(last_active_raw)
    if not active:
        return False
    return bool(active >= now_utc() - timedelta(seconds=int(within_seconds)))


def normalize_avatar_url(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if len(raw) > MAX_AVATAR_DATA_URL_LENGTH:
        return ""
    parsed = urlparse(raw)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return raw
    if raw.lower().startswith("data:image/") and ";base64," in raw:
        header, b64_part = raw.split(",", 1)
        header_l = header.lower()
        if not re.fullmatch(r"data:image/(png|jpeg|jpg|webp|gif);base64", header_l):
            return ""
        try:
            binary = base64.b64decode(b64_part, validate=True)
        except (ValueError, binascii.Error):
            return ""
        if not binary or len(binary) > MAX_AVATAR_BYTES:
            return ""
        # Keep normalized lowercase header for stable output.
        return f"{header_l},{b64_part}"
    return ""


def user_avatar_value(user_row) -> str:
    if not user_row:
        return ""
    return str(user_row["avatar_url"] or "") if "avatar_url" in user_row.keys() else ""


def user_brief_payload(user_row) -> Dict[str, Any]:
    return {
        "id": int(user_row["id"]),
        "username": user_row["username"],
        "full_name": user_row["full_name"] if "full_name" in user_row.keys() and user_row["full_name"] else user_row["username"],
        "vip_level": int(user_row["vip_level"]) if "vip_level" in user_row.keys() else 0,
        "avatar_url": user_avatar_value(user_row),
        "online": is_online_by_last_active(user_row["last_active"] if "last_active" in user_row.keys() else None),
    }


def normalize_channel_slug(value: Any) -> str:
    raw = str(value or "").strip().lower()
    raw = re.sub(r"[^a-z0-9_-]+", "-", raw)
    raw = re.sub(r"-{2,}", "-", raw).strip("-_")
    return raw[:32]


def make_unique_channel_slug(base_slug: str) -> str:
    db = get_db()
    slug = normalize_channel_slug(base_slug)
    if not slug:
        slug = "channel"
    candidate = slug
    for _ in range(200):
        exists = db.execute("SELECT id FROM messenger_channels WHERE slug = ?", (candidate,)).fetchone()
        if not exists:
            return candidate
        suffix = "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(4))
        candidate = f"{slug[:27]}-{suffix}"
    return f"{slug[:24]}-{secrets.token_hex(3)}"


def is_channel_member(channel_id: int, user_id: int) -> bool:
    row = get_db().execute(
        "SELECT id FROM messenger_channel_members WHERE channel_id = ? AND user_id = ?",
        (int(channel_id), int(user_id)),
    ).fetchone()
    return bool(row)


def purge_user_messenger_data(user_id: int) -> None:
    db = get_db()
    owned_rows = db.execute("SELECT id FROM messenger_channels WHERE owner_user_id = ?", (int(user_id),)).fetchall()
    owned_channel_ids = [int(row["id"]) for row in owned_rows]
    if owned_channel_ids:
        placeholders = ",".join("?" for _ in owned_channel_ids)
        db.execute(
            f"DELETE FROM messenger_channel_messages WHERE channel_id IN ({placeholders})",
            owned_channel_ids,
        )
        db.execute(
            f"DELETE FROM messenger_channel_members WHERE channel_id IN ({placeholders})",
            owned_channel_ids,
        )
        db.execute(
            f"DELETE FROM messenger_channels WHERE id IN ({placeholders})",
            owned_channel_ids,
        )

    db.execute("DELETE FROM messenger_channel_members WHERE user_id = ?", (int(user_id),))
    db.execute("DELETE FROM messenger_channel_messages WHERE sender_user_id = ?", (int(user_id),))
    db.execute(
        "DELETE FROM messenger_private_messages WHERE sender_user_id = ? OR receiver_user_id = ?",
        (int(user_id), int(user_id)),
    )
    db.execute(
        "DELETE FROM messenger_call_events WHERE caller_user_id = ? OR target_user_id = ?",
        (int(user_id), int(user_id)),
    )


def get_setting_row(key: str):
    return get_db().execute("SELECT key, value, updated_at FROM economy_settings WHERE key = ?", (key,)).fetchone()


def set_setting_value(key: str, value: str) -> None:
    db = get_db()
    db.execute(
        """
        INSERT INTO economy_settings (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        """,
        (key, str(value), to_iso(now_utc())),
    )
    db.commit()


def normalize_card_limits(raw_value: Any) -> Dict[str, int]:
    limits: Dict[str, int] = {str(k): int(v) for k, v in CARD_LIMITS_BY_VIP.items()}
    candidate = raw_value
    if isinstance(candidate, str):
        candidate = parse_json(candidate, {})
    if not isinstance(candidate, dict):
        return limits
    for lvl in range(0, 5):
        raw_limit = to_int(candidate.get(str(lvl), candidate.get(lvl)))
        if raw_limit is None:
            continue
        limits[str(lvl)] = max(1, min(30, int(raw_limit)))
    return limits


def get_card_limits_config() -> Dict[str, int]:
    row = get_setting_row("card_limits_by_vip")
    if not row:
        defaults = {str(k): int(v) for k, v in CARD_LIMITS_BY_VIP.items()}
        set_setting_value("card_limits_by_vip", json.dumps(defaults, ensure_ascii=False))
        return defaults
    parsed = parse_json(row["value"], {})
    limits = normalize_card_limits(parsed)
    try:
        parsed_json = json.dumps(parsed, ensure_ascii=False, sort_keys=True)
    except TypeError:
        parsed_json = ""
    normalized_json = json.dumps(limits, ensure_ascii=False, sort_keys=True)
    if parsed_json != normalized_json:
        set_setting_value("card_limits_by_vip", json.dumps(limits, ensure_ascii=False))
    return limits


def set_card_limits_config(limits: Dict[str, Any]) -> Dict[str, int]:
    normalized = normalize_card_limits(limits)
    set_setting_value("card_limits_by_vip", json.dumps(normalized, ensure_ascii=False))
    return normalized


def decimal_two(value: Any) -> Decimal:
    try:
        dec = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0.00")
    return dec.quantize(Decimal("0.01"), rounding=ROUND_DOWN)


def decimal_four(value: Any) -> Decimal:
    try:
        dec = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0.0000")
    return dec.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


def parse_usd_amount(value: Any) -> Optional[Decimal]:
    amount = decimal_two(value)
    if amount <= Decimal("0.00"):
        return None
    return amount


def parse_rate_value(value: Any) -> Optional[Decimal]:
    try:
        rate = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    rate = rate.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
    if rate < USD_RATE_MIN or rate > USD_RATE_MAX:
        return None
    return rate


def get_usd_rate(auto_update: bool = True) -> Dict[str, Any]:
    db = get_db()
    row = get_setting_row("usd_rate")
    if not row:
        set_setting_value("usd_rate", str(DEFAULT_USD_RATE))
        row = get_setting_row("usd_rate")
    rate = parse_rate_value(row["value"] if row else str(DEFAULT_USD_RATE)) or DEFAULT_USD_RATE
    updated_at = row["updated_at"] if row else to_iso(now_utc())

    if auto_update:
        updated_dt = parse_iso(updated_at)
        if (not updated_dt) or (now_utc() - updated_dt >= timedelta(seconds=USD_RATE_AUTO_SECONDS)):
            shift_percent = Decimal(str(random.uniform(float(-USD_RATE_MAX_SHIFT_PERCENT), float(USD_RATE_MAX_SHIFT_PERCENT))))
            multiplier = Decimal("1.0") + (shift_percent / Decimal("100"))
            auto_rate = (rate * multiplier).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
            auto_rate = max(USD_RATE_MIN, min(USD_RATE_MAX, auto_rate))
            db.execute(
                """
                INSERT INTO economy_settings (key, value, updated_at)
                VALUES ('usd_rate', ?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
                """,
                (str(auto_rate), to_iso(now_utc())),
            )
            db.commit()
            row = get_setting_row("usd_rate")
            rate = auto_rate
            updated_at = row["updated_at"] if row else to_iso(now_utc())

    return {
        "rate": float(rate),
        "updated_at": updated_at,
    }


def set_usd_rate_manual(new_rate: Decimal) -> Dict[str, Any]:
    rate_value = new_rate.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
    set_setting_value("usd_rate", str(rate_value))
    row = get_setting_row("usd_rate")
    return {"rate": float(rate_value), "updated_at": row["updated_at"] if row else to_iso(now_utc())}


def get_card_limit(vip_level: int) -> int:
    limits = get_card_limits_config()
    return int(limits.get(str(int(vip_level)), CARD_LIMITS_BY_VIP.get(int(vip_level), 2)))


def get_user_card_count(user_id: int) -> int:
    row = get_db().execute("SELECT COUNT(*) AS cnt FROM cards WHERE user_id = ?", (user_id,)).fetchone()
    return int(row["cnt"]) if row else 0


def format_card_number(suffix8: str) -> str:
    return f"{CARD_PREFIX} {suffix8[:4]} {suffix8[4:]}"


def create_card_number() -> str:
    db = get_db()
    for _ in range(100):
        suffix = "".join(random.choice(string.digits) for _ in range(8))
        card_number = format_card_number(suffix)
        if not db.execute("SELECT id FROM cards WHERE card_number = ?", (card_number,)).fetchone():
            return card_number
    raise ValueError("Не удалось сгенерировать уникальный номер карты")


def admin_level_name(level: int) -> str:
    return ADMIN_LEVEL_NAMES.get(int(level), "Пользователь")


def admin_capabilities(level: int, is_root: bool = False) -> Dict[str, Any]:
    lvl = int(level)
    if is_root:
        return {
            "level": 7,
            "name": "ROOT",
            "is_root": True,
            "can_use_tools": True,
            "can_support": True,
            "can_warn_ban": True,
            "can_view_logs": True,
            "can_manage_bank": True,
            "can_assign_admin": True,
            "max_assign_admin_level": 7,
            "max_ban_days": 365000,
            "can_permanent_ban": True,
        }
    return {
        "level": lvl,
        "name": admin_level_name(lvl),
        "is_root": False,
        "can_use_tools": lvl >= 2,
        "can_support": lvl >= 2,
        "can_warn_ban": lvl >= 3,
        "can_view_logs": lvl >= 4,
        "can_manage_bank": lvl >= 5,
        "can_assign_admin": MAX_GRANT_ADMIN_LEVEL.get(lvl, 0) > 0,
        "max_assign_admin_level": MAX_GRANT_ADMIN_LEVEL.get(lvl, 0),
        "max_ban_days": MAX_BAN_DAYS_BY_ADMIN_LEVEL.get(lvl, 0),
        "can_permanent_ban": lvl >= 6,
    }


def get_ban_status(user_row) -> Dict[str, Any]:
    banned_until_raw = user_row["banned_until"] if "banned_until" in user_row.keys() else None
    if isinstance(banned_until_raw, str) and banned_until_raw.strip().upper() == "PERMANENT":
        return {"is_banned": True, "banned_until": "PERMANENT", "permanent": True}
    banned_until = parse_iso(banned_until_raw)
    if banned_until and now_utc() < banned_until:
        return {"is_banned": True, "banned_until": to_iso(banned_until), "permanent": False}
    return {"is_banned": False, "banned_until": None, "permanent": False}


def clear_expired_ban(user_id: int) -> None:
    user = get_user_by_id(user_id)
    if not user:
        return
    banned_until = parse_iso(user["banned_until"] if "banned_until" in user.keys() else None)
    if banned_until and now_utc() >= banned_until:
        db = get_db()
        db.execute("UPDATE users SET banned_until = NULL WHERE id = ?", (user_id,))
        db.commit()


def calculate_level(balance: int, referral_earnings: int) -> int:
    weighted = max(balance, 0) + max(referral_earnings, 0) * 2
    return max(1, min(100, weighted // 6000 + 1))


def refresh_level(user_id: int) -> None:
    db = get_db()
    row = db.execute("SELECT balance, referral_earnings, level FROM users WHERE id = ?", (user_id,)).fetchone()
    if not row:
        return
    new_level = calculate_level(int(row["balance"]), int(row["referral_earnings"]))
    if int(row["level"]) != new_level:
        db.execute("UPDATE users SET level = ? WHERE id = ?", (new_level, user_id))
        db.commit()
        log_action(user_id, "level_update", {"new_level": new_level})


def add_achievement(user_id: int, key: str) -> None:
    if key not in ACHIEVEMENT_LABELS:
        return
    db = get_db()
    row = db.execute("SELECT achievements FROM users WHERE id = ?", (user_id,)).fetchone()
    if not row:
        return
    achievements = parse_json(row["achievements"], [])
    if not isinstance(achievements, list):
        achievements = []
    if key not in achievements:
        achievements.append(key)
        db.execute(
            "UPDATE users SET achievements = ? WHERE id = ?",
            (json.dumps(achievements, ensure_ascii=False), user_id),
        )
        db.commit()
        log_action(user_id, "achievement_unlocked", {"key": key, "label": ACHIEVEMENT_LABELS[key]})


def mark_suspicious(user_id: int, reason: str) -> None:
    db = get_db()
    row = db.execute("SELECT suspicious FROM users WHERE id = ?", (user_id,)).fetchone()
    if not row:
        return
    if int(row["suspicious"]) == 0:
        db.execute("UPDATE users SET suspicious = 1 WHERE id = ?", (user_id,))
        db.commit()
    log_action(user_id, "suspicious_mark", {"reason": reason})


def check_cooldown(user_id: int, action_name: str, cooldown_seconds: float):
    state = ANTI_ABUSE_STATE.setdefault(user_id, {})
    now_ts = time.time()
    key = "last_" + action_name
    last_ts = float(state.get(key, 0.0))
    passed = now_ts - last_ts
    if passed < cooldown_seconds:
        wait_for = round(cooldown_seconds - passed, 2)
        state["violations"] = float(state.get("violations", 0.0)) + 1.0
        state["last_violation"] = now_ts
        if int(state["violations"]) >= 3:
            mark_suspicious(user_id, "cooldown_abuse:" + action_name)
        return False, wait_for
    if float(state.get("last_violation", 0.0)) and now_ts - float(state["last_violation"]) > 300:
        state["violations"] = 0.0
    state[key] = now_ts
    return True, 0.0


def require_auth(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        user_id = session.get("user_id")
        if not user_id:
            return jsonify({"ok": False, "error": "Требуется авторизация"}), 401
        user_id = int(user_id)
        clear_expired_ban(user_id)
        user = get_user_by_id(user_id)
        if not user:
            session.clear()
            return jsonify({"ok": False, "error": "Пользователь не найден"}), 401
        ban_status = get_ban_status(user)
        is_root = is_root_user(user)
        allowed_when_banned = {
            "/api/me",
            "/api/logout",
            "/api/support/messages",
            "/api/support/send",
            "/api/account/delete",
            "/api/notifications",
            "/api/notifications/read",
            "/api/chat/public/messages",
            "/api/chat/public/send",
        }
        allowed_prefixes_when_banned = (
            "/api/messenger/",
        )
        is_allowed_path = (request.path in allowed_when_banned) or any(
            request.path.startswith(prefix) for prefix in allowed_prefixes_when_banned
        )
        if ban_status["is_banned"] and (not is_allowed_path) and not is_root:
            if ban_status.get("permanent"):
                return jsonify({"ok": False, "error": "Аккаунт заблокирован навсегда"}), 403
            return jsonify({"ok": False, "error": f"Аккаунт временно заблокирован до {ban_status['banned_until']}"}), 403
        update_last_active(user_id)
        g.user_id = user_id
        return fn(*args, **kwargs)

    return wrapper


def require_admin(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("is_admin"):
            user_id = session.get("user_id")
            user = get_user_by_id(int(user_id)) if user_id else None
            if is_admin_user(user):
                return jsonify(
                    {
                        "ok": False,
                        "error": "Нужен вход в админ-панель: откройте вкладку Админ и введите пароль админки",
                    }
                ), 403
            return jsonify({"ok": False, "error": "Требуются права администратора"}), 403
        user_id = session.get("user_id")
        user = get_user_by_id(int(user_id)) if user_id else None
        if not is_admin_user(user):
            session["is_admin"] = False
            return jsonify({"ok": False, "error": "Недостаточно прав администратора"}), 403
        clear_expired_ban(int(user_id))
        user = get_user_by_id(int(user_id))
        if not user:
            session.clear()
            return jsonify({"ok": False, "error": "Пользователь не найден"}), 401
        ban_status = get_ban_status(user)
        is_root = is_root_user(user)
        if ban_status["is_banned"] and not is_root:
            if ban_status.get("permanent"):
                return jsonify({"ok": False, "error": "Аккаунт заблокирован навсегда"}), 403
            return jsonify({"ok": False, "error": f"Аккаунт временно заблокирован до {ban_status['banned_until']}"}), 403
        g.is_root_admin = is_root
        g.admin_level = 7 if is_root else int(user["admin_level"])
        session["admin_level"] = g.admin_level
        g.admin_caps = admin_capabilities(g.admin_level, is_root)
        return fn(*args, **kwargs)

    return wrapper


def admin_level_guard(min_level: int):
    if bool(getattr(g, "is_root_admin", False)):
        return None
    level = int(getattr(g, "admin_level", 0))
    if level < min_level:
        return jsonify({"ok": False, "error": f"Нужен уровень админки {min_level}+"}), 403
    return None


def guard_root_immunity(target_user, action_label: str):
    if is_root_user(target_user) and not bool(getattr(g, "is_root_admin", False)):
        return jsonify(
            {
                "ok": False,
                "error": f"Пользователь {ROOT_USERNAME} имеет root-иммунитет, действие '{action_label}' запрещено",
            }
        ), 403
    return None


def guard_admin_hierarchy_for_moderation(target_user, action_label: str):
    denied = guard_root_immunity(target_user, action_label)
    if denied:
        return denied
    if bool(getattr(g, "is_root_admin", False)):
        return None
    actor_level = int(getattr(g, "admin_level", 0))
    target_level = int(target_user["admin_level"]) if "admin_level" in target_user.keys() else 0
    if target_level > 0 and actor_level <= target_level:
        return jsonify(
            {
                "ok": False,
                "error": f"Нельзя {action_label} админа с уровнем выше или равным вашему",
            }
        ), 403
    return None

def apply_referral_earning_bonus(user_id: int, profit: int) -> None:
    if profit <= 0:
        return
    db = get_db()
    row = db.execute("SELECT ref_by FROM users WHERE id = ?", (user_id,)).fetchone()
    if not row or not row["ref_by"]:
        return
    bonus = max(1, int(profit * 0.05))
    ref_id = int(row["ref_by"])
    db.execute(
        "UPDATE users SET balance = balance + ?, referral_earnings = referral_earnings + ? WHERE id = ?",
        (bonus, bonus, ref_id),
    )
    db.commit()
    refresh_level(ref_id)
    log_action(ref_id, "referral_profit_bonus", {"from_user_id": user_id, "source_profit": profit, "bonus": bonus})
    add_notification(
        ref_id,
        "referral_bonus",
        "Реферальный бонус",
        f"Начислен бонус {bonus} с дохода вашего реферала.",
        {"from_user_id": user_id, "source_profit": profit, "bonus": bonus},
    )


def get_online_users_count() -> int:
    threshold = to_iso(now_utc() - timedelta(seconds=60))
    row = get_db().execute(
        "SELECT COUNT(*) AS cnt FROM users WHERE last_active IS NOT NULL AND last_active >= ?",
        (threshold,),
    ).fetchone()
    return int(row["cnt"]) if row else 0


def get_active_credit(user_id: int):
    return get_db().execute(
        "SELECT * FROM credits WHERE user_id = ? AND repaid = 0 ORDER BY id DESC LIMIT 1",
        (user_id,),
    ).fetchone()


def has_penalty_log(user_id: int, credit_id: int) -> bool:
    rows = get_db().execute(
        "SELECT info FROM logs WHERE user_id = ? AND action = 'credit_penalty'",
        (user_id,),
    ).fetchall()
    for row in rows:
        payload = parse_json(row["info"], {})
        if isinstance(payload, dict) and int(payload.get("credit_id", -1)) == credit_id:
            return True
    return False


def credit_is_overdue(credit_row) -> bool:
    if not credit_row:
        return False
    due_at = parse_iso(credit_row["due_at"])
    return bool(due_at and now_utc() > due_at)


def enforce_overdue_penalty(user_id: int):
    credit = get_active_credit(user_id)
    if not credit:
        return None
    if credit_is_overdue(credit) and not has_penalty_log(user_id, int(credit["id"])):
        penalty = max(200, int(int(credit["amount"]) * 0.1))
        db = get_db()
        db.execute("UPDATE credits SET total_to_pay = total_to_pay + ? WHERE id = ?", (penalty, int(credit["id"])))
        db.commit()
        log_action(user_id, "credit_penalty", {"credit_id": int(credit["id"]), "penalty": penalty})
        add_notification(
            user_id,
            "credit_penalty",
            "Штраф по кредиту",
            f"Начислен штраф {penalty} за просроченный кредит.",
            {"credit_id": int(credit["id"]), "penalty": penalty},
        )
        mark_suspicious(user_id, "credit_overdue")
        credit = get_active_credit(user_id)
    return credit


def get_credit_terms(vip_level: int, level: int) -> Dict[str, int]:
    limit_amount = 5000 + vip_level * 6000 + level * 1200
    if vip_level == 4:
        limit_amount += 15000
    interest_percent = max(5, 25 - vip_level * 4 - min(level // 10, 5))
    deadline_days = 3 + min(vip_level, 2)
    return {
        "limit": int(limit_amount),
        "interest_percent": int(interest_percent),
        "deadline_days": int(deadline_days),
    }


def user_public_payload(user_row) -> Dict[str, Any]:
    vip_level = int(user_row["vip_level"])
    admin_level = int(user_row["admin_level"]) if "admin_level" in user_row.keys() else 0
    vip_data = VIP_CONFIG.get(vip_level, VIP_CONFIG[0])
    achievements = parse_json(user_row["achievements"], [])
    if not isinstance(achievements, list):
        achievements = []
    ban_status = get_ban_status(user_row)

    daily_remaining = 0
    last_daily = parse_iso(user_row["last_daily"])
    if last_daily:
        left = int((last_daily + timedelta(hours=24) - now_utc()).total_seconds())
        daily_remaining = max(0, left)

    active_credit = enforce_overdue_penalty(int(user_row["id"]))
    active_credit_payload = None
    games_blocked = False
    if active_credit:
        overdue = credit_is_overdue(active_credit)
        games_blocked = overdue
        active_credit_payload = {
            "id": int(active_credit["id"]),
            "amount": int(active_credit["amount"]),
            "total_to_pay": int(active_credit["total_to_pay"]),
            "taken_at": active_credit["taken_at"],
            "due_at": active_credit["due_at"],
            "repaid": bool(active_credit["repaid"]),
            "overdue": overdue,
        }

    rate_info = get_usd_rate(auto_update=True)
    balance_rub = int(user_row["balance"])
    balance_usd = float(decimal_two(user_row["balance_usd"] if "balance_usd" in user_row.keys() else 0))

    payload = {
        "id": int(user_row["id"]),
        "username": user_row["username"],
        "full_name": user_row["full_name"] if "full_name" in user_row.keys() else user_row["username"],
        "avatar_url": user_avatar_value(user_row),
        "balance": balance_rub,
        "balance_rub": balance_rub,
        "balance_usd": balance_usd,
        "default_currency": "RUB",
        "usd_rate": float(rate_info["rate"]),
        "usd_rate_updated_at": rate_info["updated_at"],
        "vip_level": vip_level,
        "vip_name": vip_data["name"],
        "vip_icon": vip_data["icon"],
        "vip_color": vip_data["color"],
        "win_chance": vip_data["win_chance"],
        "level": int(user_row["level"]),
        "achievements": achievements,
        "achievement_labels": ACHIEVEMENT_LABELS,
        "ref_code": user_row["ref_code"],
        "ref_by": user_row["ref_by"],
        "referral_earnings": int(user_row["referral_earnings"]),
        "last_daily": user_row["last_daily"],
        "daily_remaining": daily_remaining,
        "last_active": user_row["last_active"],
        "suspicious": bool(user_row["suspicious"]),
        "warnings": int(user_row["warnings"]) if "warnings" in user_row.keys() else 0,
        "is_banned": ban_status["is_banned"],
        "banned_until": ban_status["banned_until"],
        "online_users": get_online_users_count(),
        "active_credit": active_credit_payload,
        "games_blocked": games_blocked,
        "card_count": get_user_card_count(int(user_row["id"])),
        "card_limit": get_card_limit(vip_level),
        "notifications_unread": get_notifications_unread_count(int(user_row["id"])),
        "public_chat_unread": get_public_chat_unread_count(int(user_row["id"])),
    }
    if admin_level > 0:
        payload["admin_level"] = admin_level
        payload["admin_level_name"] = admin_level_name(admin_level)
        admin_panel_hash = str(user_row["admin_panel_password_hash"] or "") if "admin_panel_password_hash" in user_row.keys() else ""
        payload["admin_password_set"] = bool(admin_panel_hash.strip())
    return payload


@app.route("/")
def index_page():
    return send_from_directory(BASE_DIR, "index.html")


@app.post("/api/register")
def api_register():
    payload = request.get_json(silent=True) or {}
    username = str(payload.get("username", "")).strip()
    full_name = str(payload.get("full_name", "")).strip()
    password = str(payload.get("password", "")).strip()
    password_confirm = str(payload.get("password_confirm", "")).strip()
    ref_code = str(payload.get("ref_code", "")).strip().upper()
    if not full_name:
        full_name = username

    if not re.match(r"^[A-Za-z0-9_А-Яа-яЁё]{3,20}$", username):
        return jsonify({"ok": False, "error": "Логин: 3-20 символов (буквы/цифры/_)"}), 400
    if len(full_name) < 2 or len(full_name) > 40:
        return jsonify({"ok": False, "error": "Имя должно быть от 2 до 40 символов"}), 400
    if len(password) < 6:
        return jsonify({"ok": False, "error": "Пароль минимум 6 символов"}), 400
    if password != password_confirm:
        return jsonify({"ok": False, "error": "Пароль и подтверждение не совпадают"}), 400

    db = get_db()
    if get_user_by_username(username):
        return jsonify({"ok": False, "error": "Пользователь уже существует"}), 400

    ref_user = None
    if ref_code:
        ref_user = db.execute("SELECT id FROM users WHERE ref_code = ?", (ref_code,)).fetchone()
        if not ref_user:
            return jsonify({"ok": False, "error": "Реферальный код не найден"}), 400

    start_balance = 3000 + (1000 if ref_user else 0)
    username_lower = username.lower()
    admin_level = 7 if username_lower == ROOT_USERNAME else (6 if username_lower == "admin" else 0)
    cursor = db.execute(
        """
        INSERT INTO users (
            username, password_hash, balance, vip_level, level, achievements,
            ref_code, ref_by, referral_earnings, last_daily, last_active, suspicious, full_name, admin_level
        ) VALUES (?, ?, ?, 0, 1, '[]', ?, ?, 0, NULL, ?, 0, ?, ?)
        """,
        (
            username,
            hash_password(password),
            start_balance,
            create_ref_code(),
            int(ref_user["id"]) if ref_user else None,
            to_iso(now_utc()),
            full_name,
            admin_level,
        ),
    )
    user_id = int(cursor.lastrowid)
    db.commit()
    log_action(user_id, "register", {"username": username, "ref_by": int(ref_user["id"]) if ref_user else None})

    if ref_user:
        bonus = 1000
        ref_id = int(ref_user["id"])
        db.execute(
            "UPDATE users SET balance = balance + ?, referral_earnings = referral_earnings + ? WHERE id = ?",
            (bonus, bonus, ref_id),
        )
        db.commit()
        refresh_level(ref_id)
        log_action(ref_id, "referral_registration_bonus", {"new_user_id": user_id, "bonus": bonus})
        add_notification(
            ref_id,
            "referral_registration_bonus",
            "Реферальная регистрация",
            f"Новый реферал зарегистрирован. Начислено +{bonus}.",
            {"new_user_id": user_id, "bonus": bonus},
        )

    refresh_level(user_id)
    session["user_id"] = user_id
    return jsonify({"ok": True, "message": "Регистрация успешна", "user": user_public_payload(get_user_by_id(user_id))})


@app.post("/api/login")
def api_login():
    payload = request.get_json(silent=True) or {}
    username = str(payload.get("username", "")).strip()
    password = str(payload.get("password", "")).strip()
    user = get_user_by_username(username)
    if not user or user["password_hash"] != hash_password(password):
        log_action(None, "login_failed", {"username": username})
        return jsonify({"ok": False, "error": "Неверный логин или пароль"}), 401

    required_admin_level = 0
    login_lower = user["username"].lower()
    if login_lower == ROOT_USERNAME:
        required_admin_level = 7
    elif login_lower == "admin":
        required_admin_level = 6
    if required_admin_level and int(user["admin_level"]) < required_admin_level:
        db = get_db()
        db.execute("UPDATE users SET admin_level = ? WHERE id = ?", (required_admin_level, int(user["id"])))
        db.commit()
        user = get_user_by_id(int(user["id"]))

    session["user_id"] = int(user["id"])
    session["is_admin"] = False
    session["admin_level"] = int(user["admin_level"])
    update_last_active(int(user["id"]))
    log_action(int(user["id"]), "login", {"username": username})
    return jsonify({"ok": True, "message": "Вход выполнен", "user": user_public_payload(get_user_by_id(int(user["id"])))})


@app.post("/api/logout")
def api_logout():
    user_id = session.get("user_id")
    if user_id:
        log_action(int(user_id), "logout", {})
    session.clear()
    return jsonify({"ok": True})


@app.get("/api/me")
@require_auth
def api_me():
    return jsonify({"ok": True, "user": user_public_payload(get_user_by_id(g.user_id))})


@app.post("/api/profile/update")
@require_auth
def api_profile_update():
    payload = request.get_json(silent=True) or {}
    full_name = str(payload.get("full_name", "")).strip()
    if len(full_name) < 2 or len(full_name) > 40:
        return jsonify({"ok": False, "error": "Имя должно быть от 2 до 40 символов"}), 400

    db = get_db()
    db.execute("UPDATE users SET full_name = ? WHERE id = ?", (full_name, g.user_id))
    db.commit()
    log_action(g.user_id, "profile_updated", {"full_name": full_name})
    add_notification(g.user_id, "profile", "Профиль обновлён", f"Имя профиля изменено на: {full_name}", {"full_name": full_name})
    return jsonify({"ok": True, "message": "Профиль обновлён", "user": user_public_payload(get_user_by_id(g.user_id))})


@app.post("/api/change_password")
@require_auth
def api_change_password():
    payload = request.get_json(silent=True) or {}
    current_password = str(payload.get("current_password", "")).strip()
    new_password = str(payload.get("new_password", "")).strip()
    confirm_password = str(payload.get("confirm_password", "")).strip()

    user = get_user_by_id(g.user_id)
    if user["password_hash"] != hash_password(current_password):
        return jsonify({"ok": False, "error": "Текущий пароль неверный"}), 400
    if len(new_password) < 6:
        return jsonify({"ok": False, "error": "Новый пароль минимум 6 символов"}), 400
    if new_password != confirm_password:
        return jsonify({"ok": False, "error": "Новый пароль и подтверждение не совпадают"}), 400
    if current_password == new_password:
        return jsonify({"ok": False, "error": "Новый пароль должен отличаться от текущего"}), 400

    db = get_db()
    db.execute("UPDATE users SET password_hash = ? WHERE id = ?", (hash_password(new_password), g.user_id))
    db.commit()
    log_action(g.user_id, "password_changed", {})
    add_notification(g.user_id, "security", "Пароль изменён", "Пароль аккаунта успешно изменён.", {})
    return jsonify({"ok": True, "message": "Пароль успешно изменён"})


@app.post("/api/account/delete")
@require_auth
def api_account_delete():
    payload = request.get_json(silent=True) or {}
    current_password = str(payload.get("current_password", "")).strip()
    if len(current_password) < 1:
        return jsonify({"ok": False, "error": "Введите текущий пароль"}), 400

    user = get_user_by_id(g.user_id)
    if not user:
        session.clear()
        return jsonify({"ok": False, "error": "Пользователь не найден"}), 404
    if hash_password(current_password) != user["password_hash"]:
        return jsonify({"ok": False, "error": "Неверный текущий пароль"}), 401

    user_id = int(user["id"])
    username = user["username"]
    db = get_db()
    log_action(user_id, "account_deleted", {"username": username})
    db.execute("UPDATE users SET ref_by = NULL WHERE ref_by = ?", (user_id,))
    db.execute("DELETE FROM cards WHERE user_id = ?", (user_id,))
    db.execute("DELETE FROM credits WHERE user_id = ?", (user_id,))
    db.execute("DELETE FROM support_messages WHERE user_id = ? OR sender_user_id = ?", (user_id, user_id))
    db.execute("DELETE FROM admin_chat_messages WHERE sender_user_id = ?", (user_id,))
    db.execute("DELETE FROM public_chat_messages WHERE sender_user_id = ?", (user_id,))
    purge_user_messenger_data(user_id)
    db.execute("DELETE FROM notifications WHERE user_id = ?", (user_id,))
    db.execute("DELETE FROM users WHERE id = ?", (user_id,))
    db.commit()
    session.clear()
    return jsonify({"ok": True, "message": f"Аккаунт {username} удалён"})


@app.get("/api/cards")
@require_auth
def api_cards():
    user = get_user_by_id(g.user_id)
    rows = get_db().execute(
        "SELECT id, card_number, created_at FROM cards WHERE user_id = ? ORDER BY id DESC",
        (g.user_id,),
    ).fetchall()
    cards = [{"id": int(row["id"]), "card_number": row["card_number"], "created_at": row["created_at"]} for row in rows]
    limit_cards = get_card_limit(int(user["vip_level"]))
    rate_info = get_usd_rate(auto_update=True)
    return jsonify(
        {
            "ok": True,
            "cards": cards,
            "limit": limit_cards,
            "count": len(cards),
            "balance_rub": int(user["balance"]),
            "balance_usd": float(decimal_two(user["balance_usd"] if "balance_usd" in user.keys() else 0)),
            "usd_rate": float(rate_info["rate"]),
            "usd_rate_updated_at": rate_info["updated_at"],
        }
    )


@app.post("/api/cards/create")
@require_auth
def api_create_card():
    user = get_user_by_id(g.user_id)
    vip_level = int(user["vip_level"])
    card_limit = get_card_limit(vip_level)
    current_count = get_user_card_count(g.user_id)
    if current_count >= card_limit:
        return jsonify({"ok": False, "error": f"Лимит карт для вашего VIP: {card_limit}"}), 400

    try:
        card_number = create_card_number()
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    db = get_db()
    db.execute(
        "INSERT INTO cards (user_id, card_number, created_at) VALUES (?, ?, ?)",
        (g.user_id, card_number, to_iso(now_utc())),
    )
    db.commit()
    log_action(g.user_id, "card_created", {"card_number": card_number})
    add_notification(g.user_id, "card", "Карта создана", f"Новая карта: {card_number}", {"card_number": card_number})

    updated_count = get_user_card_count(g.user_id)
    return jsonify(
        {
            "ok": True,
            "message": "Карта создана",
            "card": {"card_number": card_number},
            "count": updated_count,
            "limit": card_limit,
            "user": user_public_payload(get_user_by_id(g.user_id)),
        }
    )


@app.get("/api/support/messages")
@require_auth
def api_support_messages():
    db = get_db()
    user = get_user_by_id(g.user_id)
    mark_read_raw = str(request.args.get("mark_read", "1")).strip().lower()
    mark_read = mark_read_raw not in {"0", "false", "no"}
    last_seen = user["support_last_seen_user"] if "support_last_seen_user" in user.keys() else None
    last_seen = last_seen or "1970-01-01T00:00:00+00:00"
    unread_row = db.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM support_messages
        WHERE user_id = ? AND sender_role = 'admin' AND timestamp > ?
        """,
        (g.user_id, last_seen),
    ).fetchone()
    unread = int(unread_row["cnt"]) if unread_row else 0

    rows = db.execute(
        """
        SELECT s.id, s.sender_role, s.message, s.timestamp, u.username
        FROM support_messages s
        JOIN users u ON u.id = s.sender_user_id
        WHERE s.user_id = ?
        ORDER BY s.id ASC
        """,
        (g.user_id,),
    ).fetchall()
    messages = [
        {
            "id": int(row["id"]),
            "sender_role": row["sender_role"],
            "sender_username": row["username"],
            "message": row["message"],
            "timestamp": row["timestamp"],
        }
        for row in rows
    ]
    if mark_read:
        db.execute("UPDATE users SET support_last_seen_user = ? WHERE id = ?", (to_iso(now_utc()), g.user_id))
        db.commit()
    return jsonify({"ok": True, "messages": messages, "unread_count": unread})


@app.post("/api/support/send")
@require_auth
def api_support_send():
    payload = request.get_json(silent=True) or {}
    message = str(payload.get("message", "")).strip()
    if len(message) < 1 or len(message) > 1000:
        return jsonify({"ok": False, "error": "Сообщение должно быть от 1 до 1000 символов"}), 400

    db = get_db()
    db.execute(
        """
        INSERT INTO support_messages (user_id, sender_user_id, sender_role, message, timestamp)
        VALUES (?, ?, 'user', ?, ?)
        """,
        (g.user_id, g.user_id, message, to_iso(now_utc())),
    )
    db.commit()
    log_action(g.user_id, "support_message_sent", {"length": len(message)})
    return jsonify({"ok": True, "message": "Сообщение отправлено в поддержку"})


@app.get("/api/notifications")
@require_auth
def api_notifications():
    limit = to_int(request.args.get("limit"), 100)
    if limit is None:
        limit = 100
    limit = max(1, min(limit, 300))
    mark_read_raw = str(request.args.get("mark_read", "0")).strip().lower()
    mark_read = mark_read_raw in {"1", "true", "yes"}

    db = get_db()
    rows = db.execute(
        """
        SELECT id, kind, title, message, info, timestamp, read
        FROM notifications
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (g.user_id, limit),
    ).fetchall()
    items = [
        {
            "id": int(row["id"]),
            "kind": row["kind"],
            "title": row["title"],
            "message": row["message"],
            "info": parse_json(row["info"], {}),
            "timestamp": row["timestamp"],
            "read": bool(row["read"]),
        }
        for row in rows
    ]

    if mark_read:
        db.execute("UPDATE notifications SET read = 1 WHERE user_id = ? AND read = 0", (g.user_id,))
        db.commit()
        unread_count = 0
    else:
        unread_count = get_notifications_unread_count(g.user_id)
    return jsonify({"ok": True, "notifications": items, "unread_count": unread_count})


@app.post("/api/notifications/read")
@require_auth
def api_notifications_read():
    payload = request.get_json(silent=True) or {}
    mark_all = bool(payload.get("all"))
    ids = payload.get("ids")
    db = get_db()
    if mark_all:
        db.execute("UPDATE notifications SET read = 1 WHERE user_id = ? AND read = 0", (g.user_id,))
        db.commit()
    elif isinstance(ids, list):
        clean_ids: List[int] = []
        for raw in ids:
            val = to_int(raw)
            if val is not None and val > 0:
                clean_ids.append(val)
        if clean_ids:
            placeholders = ",".join("?" for _ in clean_ids)
            db.execute(
                f"UPDATE notifications SET read = 1 WHERE user_id = ? AND id IN ({placeholders})",
                [g.user_id, *clean_ids],
            )
            db.commit()
    unread_count = get_notifications_unread_count(g.user_id)
    return jsonify({"ok": True, "unread_count": unread_count})


@app.get("/api/chat/public/messages")
@require_auth
def api_public_chat_messages():
    limit = to_int(request.args.get("limit"), 200)
    if limit is None:
        limit = 200
    limit = max(1, min(limit, 300))
    mark_read_raw = str(request.args.get("mark_read", "1")).strip().lower()
    mark_read = mark_read_raw not in {"0", "false", "no"}

    db = get_db()
    me = get_user_by_id(g.user_id)
    last_seen = me["public_chat_last_seen"] if "public_chat_last_seen" in me.keys() else None
    last_seen = last_seen or "1970-01-01T00:00:00+00:00"
    unread_row = db.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM public_chat_messages
        WHERE sender_user_id != ? AND timestamp > ?
        """,
        (g.user_id, last_seen),
    ).fetchone()
    unread_count = int(unread_row["cnt"]) if unread_row else 0

    rows = db.execute(
        """
        SELECT c.id, c.message, c.timestamp, u.id AS sender_id, u.username, u.vip_level, u.admin_level, u.avatar_url
        FROM public_chat_messages c
        JOIN users u ON u.id = c.sender_user_id
        ORDER BY c.id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    messages = [
        {
            "id": int(row["id"]),
            "message": row["message"],
            "timestamp": row["timestamp"],
            "sender_id": int(row["sender_id"]),
            "sender_username": row["username"],
            "sender_vip_level": int(row["vip_level"]),
            "sender_admin_level": int(row["admin_level"]),
            "sender_avatar_url": str(row["avatar_url"] or ""),
        }
        for row in reversed(rows)
    ]

    if mark_read:
        db.execute("UPDATE users SET public_chat_last_seen = ? WHERE id = ?", (to_iso(now_utc()), g.user_id))
        db.commit()
        unread_count = 0
    return jsonify({"ok": True, "messages": messages, "unread_count": unread_count})


@app.post("/api/chat/public/send")
@require_auth
def api_public_chat_send():
    payload = request.get_json(silent=True) or {}
    message = str(payload.get("message", "")).strip()
    if len(message) < 1 or len(message) > 1000:
        return jsonify({"ok": False, "error": "Сообщение должно быть от 1 до 1000 символов"}), 400

    db = get_db()
    db.execute(
        "INSERT INTO public_chat_messages (sender_user_id, message, timestamp) VALUES (?, ?, ?)",
        (g.user_id, message, to_iso(now_utc())),
    )
    db.commit()
    log_action(g.user_id, "public_chat_send", {"length": len(message)})
    return jsonify({"ok": True, "message": "Сообщение отправлено"})


@app.post("/api/messenger/avatar")
@require_auth
def api_messenger_set_avatar():
    payload = request.get_json(silent=True) or {}
    raw_avatar_data = str(payload.get("avatar_data", "")).strip()
    raw_avatar_url = str(payload.get("avatar_url", "")).strip()
    raw_avatar = raw_avatar_data or raw_avatar_url
    avatar_url = normalize_avatar_url(raw_avatar)
    if raw_avatar and not avatar_url:
        return jsonify({"ok": False, "error": "Аватар должен быть http/https URL или изображением до 1.5MB"}), 400

    db = get_db()
    db.execute("UPDATE users SET avatar_url = ? WHERE id = ?", (avatar_url, g.user_id))
    db.commit()
    log_action(g.user_id, "messenger_avatar_set", {"has_avatar": bool(avatar_url)})
    return jsonify({"ok": True, "message": "Аватар обновлён", "user": user_public_payload(get_user_by_id(g.user_id))})


@app.get("/api/messenger/users/search")
@require_auth
def api_messenger_search_users():
    q = str(request.args.get("q", "")).strip().lower()
    if not q:
        return jsonify({"ok": True, "users": []})
    if len(q) > 30:
        q = q[:30]

    pattern = f"%{q}%"
    rows = get_db().execute(
        """
        SELECT id, username, full_name, vip_level, avatar_url, last_active
        FROM users
        WHERE (lower(username) LIKE ? OR lower(full_name) LIKE ?)
        ORDER BY
            CASE
                WHEN lower(username) = ? THEN 0
                WHEN lower(username) LIKE ? THEN 1
                ELSE 2
            END,
            id DESC
        LIMIT 30
        """,
        (pattern, pattern, q, f"{q}%"),
    ).fetchall()

    users = []
    for row in rows:
        payload = user_brief_payload(row)
        payload["vip_name"] = VIP_CONFIG.get(int(payload["vip_level"]), VIP_CONFIG[0])["name"]
        users.append(payload)
    return jsonify({"ok": True, "users": users})


@app.get("/api/messenger/dialogs")
@require_auth
def api_messenger_dialogs():
    limit = to_int(request.args.get("limit"), 30)
    if limit is None:
        limit = 30
    limit = max(1, min(limit, 100))

    rows = get_db().execute(
        """
        SELECT id, sender_user_id, receiver_user_id, message, timestamp
        FROM messenger_private_messages
        WHERE sender_user_id = ? OR receiver_user_id = ?
        ORDER BY id DESC
        LIMIT 800
        """,
        (g.user_id, g.user_id),
    ).fetchall()

    latest_by_peer: Dict[int, sqlite3.Row] = {}
    order: List[int] = []
    for row in rows:
        peer_id = int(row["receiver_user_id"] if int(row["sender_user_id"]) == g.user_id else row["sender_user_id"])
        if peer_id not in latest_by_peer:
            latest_by_peer[peer_id] = row
            order.append(peer_id)
            if len(order) >= limit:
                break

    if not order:
        return jsonify({"ok": True, "dialogs": []})

    placeholders = ",".join("?" for _ in order)
    peer_rows = get_db().execute(
        f"""
        SELECT id, username, full_name, vip_level, avatar_url, last_active
        FROM users
        WHERE id IN ({placeholders})
        """,
        order,
    ).fetchall()
    peer_map = {int(row["id"]): row for row in peer_rows}

    dialogs = []
    for peer_id in order:
        peer_row = peer_map.get(peer_id)
        if not peer_row:
            continue
        message_row = latest_by_peer[peer_id]
        dialogs.append(
            {
                "peer": user_brief_payload(peer_row),
                "last_message": message_row["message"],
                "timestamp": message_row["timestamp"],
                "from_me": bool(int(message_row["sender_user_id"]) == g.user_id),
            }
        )
    return jsonify({"ok": True, "dialogs": dialogs})


@app.get("/api/messenger/dialog/<int:peer_user_id>")
@require_auth
def api_messenger_dialog(peer_user_id: int):
    if int(peer_user_id) == g.user_id:
        return jsonify({"ok": False, "error": "Нельзя открыть диалог с собой"}), 400
    peer = get_user_by_id(int(peer_user_id))
    if not peer:
        return jsonify({"ok": False, "error": "Пользователь не найден"}), 404

    limit = to_int(request.args.get("limit"), 200)
    if limit is None:
        limit = 200
    limit = max(1, min(limit, 300))
    rows = get_db().execute(
        """
        SELECT m.id, m.sender_user_id, m.receiver_user_id, m.message, m.timestamp,
               u.username AS sender_username, u.full_name AS sender_full_name, u.vip_level AS sender_vip_level, u.avatar_url AS sender_avatar_url
        FROM messenger_private_messages m
        JOIN users u ON u.id = m.sender_user_id
        WHERE (m.sender_user_id = ? AND m.receiver_user_id = ?)
           OR (m.sender_user_id = ? AND m.receiver_user_id = ?)
        ORDER BY m.id DESC
        LIMIT ?
        """,
        (g.user_id, int(peer_user_id), int(peer_user_id), g.user_id, limit),
    ).fetchall()

    messages = [
        {
            "id": int(row["id"]),
            "sender_id": int(row["sender_user_id"]),
            "receiver_id": int(row["receiver_user_id"]),
            "sender_username": row["sender_username"],
            "sender_full_name": row["sender_full_name"],
            "sender_vip_level": int(row["sender_vip_level"]),
            "sender_avatar_url": str(row["sender_avatar_url"] or ""),
            "message": row["message"],
            "timestamp": row["timestamp"],
            "from_me": bool(int(row["sender_user_id"]) == g.user_id),
        }
        for row in reversed(rows)
    ]
    return jsonify({"ok": True, "peer": user_brief_payload(peer), "messages": messages})


@app.post("/api/messenger/dialog/send")
@require_auth
def api_messenger_send_dialog_message():
    payload = request.get_json(silent=True) or {}
    peer_user_id = to_int(payload.get("to_user_id"))
    message = str(payload.get("message", "")).strip()
    if peer_user_id is None:
        return jsonify({"ok": False, "error": "Укажите получателя"}), 400
    if int(peer_user_id) == g.user_id:
        return jsonify({"ok": False, "error": "Нельзя писать самому себе"}), 400
    if len(message) < 1 or len(message) > 1000:
        return jsonify({"ok": False, "error": "Сообщение должно быть от 1 до 1000 символов"}), 400

    peer = get_user_by_id(int(peer_user_id))
    if not peer:
        return jsonify({"ok": False, "error": "Пользователь не найден"}), 404

    db = get_db()
    timestamp = to_iso(now_utc())
    cursor = db.execute(
        """
        INSERT INTO messenger_private_messages (sender_user_id, receiver_user_id, message, timestamp)
        VALUES (?, ?, ?, ?)
        """,
        (g.user_id, int(peer_user_id), message, timestamp),
    )
    db.commit()

    me = get_user_by_id(g.user_id)
    preview = message if len(message) <= 120 else (message[:117] + "...")
    add_notification(
        int(peer_user_id),
        "messenger_dm",
        f"Сообщение от {me['username']}",
        preview,
        {"from_user_id": g.user_id},
    )
    log_action(g.user_id, "messenger_dm_send", {"to_user_id": int(peer_user_id), "length": len(message)})
    return jsonify({"ok": True, "message": "Сообщение отправлено", "message_id": int(cursor.lastrowid), "timestamp": timestamp})


@app.get("/api/messenger/channels")
@require_auth
def api_messenger_channels():
    db = get_db()
    joined_rows = db.execute(
        """
        SELECT c.id, c.slug, c.title, c.owner_user_id, c.created_at, c.is_public,
               owner.username AS owner_username,
               (SELECT COUNT(*) FROM messenger_channel_members mm WHERE mm.channel_id = c.id) AS members_count,
               COALESCE((SELECT MAX(timestamp) FROM messenger_channel_messages mcm WHERE mcm.channel_id = c.id), c.created_at) AS last_activity
        FROM messenger_channels c
        JOIN messenger_channel_members me ON me.channel_id = c.id AND me.user_id = ?
        JOIN users owner ON owner.id = c.owner_user_id
        ORDER BY last_activity DESC, c.id DESC
        LIMIT 120
        """,
        (g.user_id,),
    ).fetchall()

    discover_rows = db.execute(
        """
        SELECT c.id, c.slug, c.title, c.owner_user_id, c.created_at, c.is_public,
               owner.username AS owner_username,
               (SELECT COUNT(*) FROM messenger_channel_members mm WHERE mm.channel_id = c.id) AS members_count
        FROM messenger_channels c
        JOIN users owner ON owner.id = c.owner_user_id
        WHERE c.is_public = 1
          AND c.id NOT IN (SELECT channel_id FROM messenger_channel_members WHERE user_id = ?)
        ORDER BY c.id DESC
        LIMIT 120
        """,
        (g.user_id,),
    ).fetchall()

    joined = [
        {
            "id": int(row["id"]),
            "slug": row["slug"],
            "title": row["title"],
            "owner_user_id": int(row["owner_user_id"]),
            "owner_username": row["owner_username"],
            "created_at": row["created_at"],
            "is_public": bool(row["is_public"]),
            "members_count": int(row["members_count"] or 0),
            "last_activity": row["last_activity"],
        }
        for row in joined_rows
    ]
    discover = [
        {
            "id": int(row["id"]),
            "slug": row["slug"],
            "title": row["title"],
            "owner_user_id": int(row["owner_user_id"]),
            "owner_username": row["owner_username"],
            "created_at": row["created_at"],
            "is_public": bool(row["is_public"]),
            "members_count": int(row["members_count"] or 0),
        }
        for row in discover_rows
    ]
    return jsonify({"ok": True, "joined": joined, "discover": discover})


@app.post("/api/messenger/channels/create")
@require_auth
def api_messenger_create_channel():
    payload = request.get_json(silent=True) or {}
    title = str(payload.get("title", "")).strip()
    slug_input = str(payload.get("slug", "")).strip()
    is_public = bool(payload.get("is_public", True))

    if len(title) < 2 or len(title) > 60:
        return jsonify({"ok": False, "error": "Название канала: от 2 до 60 символов"}), 400

    base_slug = slug_input or title
    slug = make_unique_channel_slug(base_slug)
    now_iso = to_iso(now_utc())
    db = get_db()
    cur = db.execute(
        """
        INSERT INTO messenger_channels (slug, title, owner_user_id, is_public, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (slug, title, g.user_id, 1 if is_public else 0, now_iso),
    )
    channel_id = int(cur.lastrowid)
    db.execute(
        "INSERT INTO messenger_channel_members (channel_id, user_id, joined_at) VALUES (?, ?, ?)",
        (channel_id, g.user_id, now_iso),
    )
    db.commit()
    log_action(g.user_id, "messenger_channel_create", {"channel_id": channel_id, "slug": slug})
    return jsonify(
        {
            "ok": True,
            "message": "Канал создан",
            "channel": {
                "id": channel_id,
                "slug": slug,
                "title": title,
                "owner_user_id": g.user_id,
                "owner_username": get_user_by_id(g.user_id)["username"],
                "created_at": now_iso,
                "is_public": bool(is_public),
                "members_count": 1,
            },
        }
    )


@app.post("/api/messenger/channels/join")
@require_auth
def api_messenger_join_channel():
    payload = request.get_json(silent=True) or {}
    channel_id = to_int(payload.get("channel_id"))
    if channel_id is None:
        return jsonify({"ok": False, "error": "Укажите channel_id"}), 400
    db = get_db()
    channel = db.execute(
        "SELECT id, title, is_public, owner_user_id FROM messenger_channels WHERE id = ?",
        (int(channel_id),),
    ).fetchone()
    if not channel:
        return jsonify({"ok": False, "error": "Канал не найден"}), 404
    if (not bool(channel["is_public"])) and int(channel["owner_user_id"]) != g.user_id:
        return jsonify({"ok": False, "error": "Это приватный канал"}), 403

    exists = db.execute(
        "SELECT id FROM messenger_channel_members WHERE channel_id = ? AND user_id = ?",
        (int(channel_id), g.user_id),
    ).fetchone()
    if exists:
        return jsonify({"ok": True, "message": "Вы уже подписаны на канал"})

    db.execute(
        "INSERT INTO messenger_channel_members (channel_id, user_id, joined_at) VALUES (?, ?, ?)",
        (int(channel_id), g.user_id, to_iso(now_utc())),
    )
    db.commit()
    log_action(g.user_id, "messenger_channel_join", {"channel_id": int(channel_id)})
    return jsonify({"ok": True, "message": f"Вы подписались на канал {channel['title']}"})


@app.get("/api/messenger/channels/<int:channel_id>/messages")
@require_auth
def api_messenger_channel_messages(channel_id: int):
    if not is_channel_member(int(channel_id), g.user_id):
        return jsonify({"ok": False, "error": "Нет доступа к этому каналу"}), 403
    db = get_db()
    channel = db.execute(
        """
        SELECT c.id, c.slug, c.title, c.owner_user_id, c.created_at, c.is_public, u.username AS owner_username
        FROM messenger_channels c
        JOIN users u ON u.id = c.owner_user_id
        WHERE c.id = ?
        """,
        (int(channel_id),),
    ).fetchone()
    if not channel:
        return jsonify({"ok": False, "error": "Канал не найден"}), 404

    limit = to_int(request.args.get("limit"), 250)
    if limit is None:
        limit = 250
    limit = max(1, min(limit, 400))
    rows = db.execute(
        """
        SELECT m.id, m.message, m.timestamp, m.sender_user_id,
               u.username AS sender_username, u.full_name AS sender_full_name, u.vip_level AS sender_vip_level, u.avatar_url AS sender_avatar_url
        FROM messenger_channel_messages m
        JOIN users u ON u.id = m.sender_user_id
        WHERE m.channel_id = ?
        ORDER BY m.id DESC
        LIMIT ?
        """,
        (int(channel_id), limit),
    ).fetchall()

    messages = [
        {
            "id": int(row["id"]),
            "channel_id": int(channel_id),
            "sender_id": int(row["sender_user_id"]),
            "sender_username": row["sender_username"],
            "sender_full_name": row["sender_full_name"],
            "sender_vip_level": int(row["sender_vip_level"]),
            "sender_avatar_url": str(row["sender_avatar_url"] or ""),
            "message": row["message"],
            "timestamp": row["timestamp"],
            "from_me": bool(int(row["sender_user_id"]) == g.user_id),
        }
        for row in reversed(rows)
    ]

    return jsonify(
        {
            "ok": True,
            "channel": {
                "id": int(channel["id"]),
                "slug": channel["slug"],
                "title": channel["title"],
                "owner_user_id": int(channel["owner_user_id"]),
                "owner_username": channel["owner_username"],
                "created_at": channel["created_at"],
                "is_public": bool(channel["is_public"]),
            },
            "messages": messages,
        }
    )


@app.post("/api/messenger/channels/<int:channel_id>/send")
@require_auth
def api_messenger_channel_send(channel_id: int):
    if not is_channel_member(int(channel_id), g.user_id):
        return jsonify({"ok": False, "error": "Нет доступа к этому каналу"}), 403
    payload = request.get_json(silent=True) or {}
    message = str(payload.get("message", "")).strip()
    if len(message) < 1 or len(message) > 1000:
        return jsonify({"ok": False, "error": "Сообщение должно быть от 1 до 1000 символов"}), 400

    db = get_db()
    channel = db.execute("SELECT title FROM messenger_channels WHERE id = ?", (int(channel_id),)).fetchone()
    if not channel:
        return jsonify({"ok": False, "error": "Канал не найден"}), 404

    now_iso = to_iso(now_utc())
    db.execute(
        """
        INSERT INTO messenger_channel_messages (channel_id, sender_user_id, message, timestamp)
        VALUES (?, ?, ?, ?)
        """,
        (int(channel_id), g.user_id, message, now_iso),
    )
    db.commit()

    me = get_user_by_id(g.user_id)
    preview = message if len(message) <= 120 else (message[:117] + "...")
    member_rows = db.execute(
        "SELECT user_id FROM messenger_channel_members WHERE channel_id = ? AND user_id != ?",
        (int(channel_id), g.user_id),
    ).fetchall()
    for row in member_rows:
        add_notification(
            int(row["user_id"]),
            "messenger_channel",
            f"Канал #{channel['title']}",
            f"{me['username']}: {preview}",
            {"channel_id": int(channel_id), "from_user_id": g.user_id},
        )

    log_action(g.user_id, "messenger_channel_send", {"channel_id": int(channel_id), "length": len(message)})
    return jsonify({"ok": True, "message": "Сообщение в канал отправлено", "timestamp": now_iso})


@app.post("/api/messenger/call/start")
@require_auth
def api_messenger_call_start():
    payload = request.get_json(silent=True) or {}
    target_user_id = to_int(payload.get("target_user_id"))
    if target_user_id is None:
        return jsonify({"ok": False, "error": "Укажите target_user_id"}), 400
    if int(target_user_id) == g.user_id:
        return jsonify({"ok": False, "error": "Нельзя звонить самому себе"}), 400

    target = get_user_by_id(int(target_user_id))
    if not target:
        return jsonify({"ok": False, "error": "Пользователь не найден"}), 404

    now_iso = to_iso(now_utc())
    db = get_db()
    cur = db.execute(
        """
        INSERT INTO messenger_call_events (caller_user_id, target_user_id, status, timestamp)
        VALUES (?, ?, ?, ?)
        """,
        (g.user_id, int(target_user_id), "ringing", now_iso),
    )
    db.commit()

    caller = get_user_by_id(g.user_id)
    add_notification(
        int(target_user_id),
        "messenger_call",
        "Входящий звонок",
        f"{caller['username']} пытается дозвониться",
        {"from_user_id": g.user_id, "call_id": int(cur.lastrowid)},
    )
    log_action(g.user_id, "messenger_call_start", {"target_user_id": int(target_user_id), "call_id": int(cur.lastrowid)})
    return jsonify(
        {
            "ok": True,
            "message": f"Вызов отправлен пользователю {target['username']}",
            "call": {
                "id": int(cur.lastrowid),
                "caller_user_id": g.user_id,
                "target_user_id": int(target_user_id),
                "status": "ringing",
                "timestamp": now_iso,
            },
        }
    )


@app.post("/api/messenger/call/end")
@require_auth
def api_messenger_call_end():
    payload = request.get_json(silent=True) or {}
    call_id = to_int(payload.get("call_id"))
    if call_id is None:
        return jsonify({"ok": False, "error": "Укажите call_id"}), 400

    status = str(payload.get("status", "ended")).strip().lower()
    if status not in {"ended", "cancelled", "rejected", "missed"}:
        status = "ended"

    db = get_db()
    call = db.execute(
        """
        SELECT id, caller_user_id, target_user_id, status, timestamp
        FROM messenger_call_events
        WHERE id = ? AND (caller_user_id = ? OR target_user_id = ?)
        """,
        (int(call_id), g.user_id, g.user_id),
    ).fetchone()
    if not call:
        return jsonify({"ok": False, "error": "Звонок не найден"}), 404

    current_status = str(call["status"] or "").lower()
    if current_status in {"ended", "cancelled", "rejected", "missed"}:
        return jsonify(
            {
                "ok": True,
                "message": "Звонок уже завершён",
                "call": {
                    "id": int(call["id"]),
                    "caller_user_id": int(call["caller_user_id"]),
                    "target_user_id": int(call["target_user_id"]),
                    "status": current_status,
                    "timestamp": call["timestamp"],
                },
            }
        )

    db.execute("UPDATE messenger_call_events SET status = ? WHERE id = ?", (status, int(call_id)))
    db.commit()

    other_user_id = int(call["target_user_id"]) if int(call["caller_user_id"]) == g.user_id else int(call["caller_user_id"])
    actor = get_user_by_id(g.user_id)
    add_notification(
        other_user_id,
        "messenger_call",
        "Звонок завершён",
        f"{actor['username']} завершил звонок",
        {"from_user_id": g.user_id, "call_id": int(call_id), "status": status},
    )
    log_action(g.user_id, "messenger_call_end", {"call_id": int(call_id), "status": status})
    return jsonify(
        {
            "ok": True,
            "message": "Звонок завершён",
            "call": {
                "id": int(call["id"]),
                "caller_user_id": int(call["caller_user_id"]),
                "target_user_id": int(call["target_user_id"]),
                "status": status,
                "timestamp": call["timestamp"],
            },
        }
    )


@app.get("/api/messenger/calls")
@require_auth
def api_messenger_calls():
    limit = to_int(request.args.get("limit"), 40)
    if limit is None:
        limit = 40
    limit = max(1, min(limit, 120))

    rows = get_db().execute(
        """
        SELECT c.id, c.caller_user_id, c.target_user_id, c.status, c.timestamp,
               u1.username AS caller_username, u1.avatar_url AS caller_avatar_url,
               u2.username AS target_username, u2.avatar_url AS target_avatar_url
        FROM messenger_call_events c
        JOIN users u1 ON u1.id = c.caller_user_id
        JOIN users u2 ON u2.id = c.target_user_id
        WHERE c.caller_user_id = ? OR c.target_user_id = ?
        ORDER BY c.id DESC
        LIMIT ?
        """,
        (g.user_id, g.user_id, limit),
    ).fetchall()
    calls = [
        {
            "id": int(row["id"]),
            "caller_user_id": int(row["caller_user_id"]),
            "target_user_id": int(row["target_user_id"]),
            "caller_username": row["caller_username"],
            "caller_avatar_url": str(row["caller_avatar_url"] or ""),
            "target_username": row["target_username"],
            "target_avatar_url": str(row["target_avatar_url"] or ""),
            "status": row["status"],
            "timestamp": row["timestamp"],
            "incoming": bool(int(row["target_user_id"]) == g.user_id),
        }
        for row in rows
    ]
    return jsonify({"ok": True, "calls": calls})


@app.get("/api/currency")
@require_auth
def api_currency_info():
    user = get_user_by_id(g.user_id)
    rate_info = get_usd_rate(auto_update=True)
    return jsonify(
        {
            "ok": True,
            "default_currency": "RUB",
            "usd_rate": float(rate_info["rate"]),
            "usd_rate_updated_at": rate_info["updated_at"],
            "balance_rub": int(user["balance"]),
            "balance_usd": float(decimal_two(user["balance_usd"] if "balance_usd" in user.keys() else 0)),
        }
    )


@app.post("/api/currency/convert")
@require_auth
def api_currency_convert():
    payload = request.get_json(silent=True) or {}
    direction = str(payload.get("direction", "")).strip().upper()
    rate_info = get_usd_rate(auto_update=True)
    rate = Decimal(str(rate_info["rate"]))
    user = get_user_by_id(g.user_id)
    balance_rub = int(user["balance"])
    balance_usd = decimal_two(user["balance_usd"] if "balance_usd" in user.keys() else 0)

    db = get_db()
    if direction == "RUB_TO_USD":
        try:
            amount_rub_dec = Decimal(str(payload.get("amount")))
        except (InvalidOperation, ValueError, TypeError):
            amount_rub_dec = Decimal("-1")
        amount_rub = int(amount_rub_dec) if amount_rub_dec == amount_rub_dec.quantize(Decimal("1"), rounding=ROUND_DOWN) else None
        if amount_rub is None or amount_rub <= 0:
            return jsonify({"ok": False, "error": "Сумма RUB должна быть больше 0"}), 400
        if balance_rub < amount_rub:
            return jsonify({"ok": False, "error": "Недостаточно RUB"}), 400
        usd_gain = (Decimal(amount_rub) / rate).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        if usd_gain <= Decimal("0.00"):
            return jsonify({"ok": False, "error": "Слишком маленькая сумма для конвертации"}), 400

        new_rub = balance_rub - amount_rub
        new_usd = (balance_usd + usd_gain).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        db.execute(
            "UPDATE users SET balance = ?, balance_usd = ? WHERE id = ?",
            (new_rub, float(new_usd), g.user_id),
        )
        db.commit()
        refresh_level(g.user_id)
        updated = get_user_by_id(g.user_id)
        log_action(
            g.user_id,
            "currency_convert_rub_to_usd",
            {
                "amount_rub": amount_rub,
                "usd_gain": float(usd_gain),
                "usd_rate": float(rate),
                "balance_after_rub": int(updated["balance"]),
                "balance_after_usd": float(decimal_two(updated["balance_usd"] if "balance_usd" in updated.keys() else 0)),
            },
        )
        add_notification(
            g.user_id,
            "currency_convert",
            "Конвертация валюты",
            f"Конвертация: -{amount_rub} RUB, +{float(usd_gain):.2f} USD",
            {"direction": direction, "amount_rub": amount_rub, "usd_gain": float(usd_gain), "usd_rate": float(rate)},
        )
        return jsonify(
            {
                "ok": True,
                "message": f"Конвертировано: -{amount_rub} RUB, +{float(usd_gain):.2f} USD",
                "usd_rate": float(rate_info["rate"]),
                "user": user_public_payload(updated),
            }
        )

    if direction == "USD_TO_RUB":
        amount_usd = parse_usd_amount(payload.get("amount"))
        if amount_usd is None:
            return jsonify({"ok": False, "error": "Сумма USD должна быть больше 0"}), 400
        if balance_usd < amount_usd:
            return jsonify({"ok": False, "error": "Недостаточно USD"}), 400
        rub_gain = int((amount_usd * rate).quantize(Decimal("1"), rounding=ROUND_DOWN))
        if rub_gain <= 0:
            return jsonify({"ok": False, "error": "Слишком маленькая сумма для конвертации"}), 400

        new_rub = balance_rub + rub_gain
        new_usd = (balance_usd - amount_usd).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        db.execute(
            "UPDATE users SET balance = ?, balance_usd = ? WHERE id = ?",
            (new_rub, float(new_usd), g.user_id),
        )
        db.commit()
        refresh_level(g.user_id)
        updated = get_user_by_id(g.user_id)
        log_action(
            g.user_id,
            "currency_convert_usd_to_rub",
            {
                "amount_usd": float(amount_usd),
                "rub_gain": rub_gain,
                "usd_rate": float(rate),
                "balance_after_rub": int(updated["balance"]),
                "balance_after_usd": float(decimal_two(updated["balance_usd"] if "balance_usd" in updated.keys() else 0)),
            },
        )
        add_notification(
            g.user_id,
            "currency_convert",
            "Конвертация валюты",
            f"Конвертация: -{float(amount_usd):.2f} USD, +{rub_gain} RUB",
            {"direction": direction, "amount_usd": float(amount_usd), "rub_gain": rub_gain, "usd_rate": float(rate)},
        )
        return jsonify(
            {
                "ok": True,
                "message": f"Конвертировано: -{float(amount_usd):.2f} USD, +{rub_gain} RUB",
                "usd_rate": float(rate_info["rate"]),
                "user": user_public_payload(updated),
            }
        )

    return jsonify({"ok": False, "error": "Некорректное направление конвертации"}), 400


@app.post("/api/click")
@require_auth
def api_click():
    allowed, wait_for = check_cooldown(g.user_id, "click", CLICK_COOLDOWN_SECONDS)
    if not allowed:
        log_action(g.user_id, "click_rate_limited", {"wait_for": wait_for})
        return jsonify({"ok": False, "error": f"Клик раз в 1с ({wait_for}с)"}), 429

    user = get_user_by_id(g.user_id)
    amount = random.randint(20, 60)
    if int(user["vip_level"]) >= 1:
        amount = int(amount * 1.05)

    db = get_db()
    db.execute("UPDATE users SET balance = balance + ? WHERE id = ?", (amount, g.user_id))
    db.commit()
    refresh_level(g.user_id)
    updated = get_user_by_id(g.user_id)

    log_action(g.user_id, "click", {"amount": amount, "balance_after": int(updated["balance"])})
    apply_referral_earning_bonus(g.user_id, amount)
    add_achievement(g.user_id, "first_click")
    return jsonify({"ok": True, "message": f"+{amount} к балансу", "amount": amount, "user": user_public_payload(updated)})


@app.post("/api/daily")
@require_auth
def api_daily():
    user = get_user_by_id(g.user_id)
    now_value = now_utc()
    last_daily = parse_iso(user["last_daily"])
    if last_daily and now_value < last_daily + timedelta(hours=24):
        left = int((last_daily + timedelta(hours=24) - now_value).total_seconds())
        return jsonify({"ok": False, "error": f"Ежедневный бонус через {left}с"}), 400

    bonus = 1800
    if int(user["vip_level"]) >= 2:
        bonus = int(bonus * 1.1)

    db = get_db()
    db.execute("UPDATE users SET balance = balance + ?, last_daily = ? WHERE id = ?", (bonus, to_iso(now_value), g.user_id))
    db.commit()

    refresh_level(g.user_id)
    updated = get_user_by_id(g.user_id)
    log_action(g.user_id, "daily_bonus", {"amount": bonus, "balance_after": int(updated["balance"])})
    add_notification(g.user_id, "daily", "Ежедневный бонус", f"На баланс начислено +{bonus}", {"amount": bonus})
    apply_referral_earning_bonus(g.user_id, bonus)
    return jsonify({"ok": True, "message": f"Ежедневный бонус +{bonus}", "amount": bonus, "user": user_public_payload(updated)})

@app.post("/api/transfer")
@require_auth
def api_transfer():
    payload = request.get_json(silent=True) or {}
    to_username = str(payload.get("to_username", "")).strip()
    currency = str(payload.get("currency", "RUB")).strip().upper()
    amount_raw = payload.get("amount")
    amount_rub = None
    if currency == "RUB":
        try:
            amount_rub_dec = Decimal(str(amount_raw))
        except (InvalidOperation, ValueError, TypeError):
            amount_rub_dec = Decimal("-1")
        if amount_rub_dec == amount_rub_dec.quantize(Decimal("1"), rounding=ROUND_DOWN):
            amount_rub = int(amount_rub_dec)
    amount_usd = parse_usd_amount(amount_raw) if currency == "USD" else None

    if currency not in {"RUB", "USD"}:
        return jsonify({"ok": False, "error": "Валюта перевода должна быть RUB или USD"}), 400
    if not to_username:
        return jsonify({"ok": False, "error": "Укажите получателя"}), 400
    if currency == "RUB" and (amount_rub is None or amount_rub <= 0):
        return jsonify({"ok": False, "error": "Сумма RUB должна быть больше 0"}), 400
    if currency == "USD" and amount_usd is None:
        return jsonify({"ok": False, "error": "Сумма USD должна быть больше 0"}), 400

    sender = get_user_by_id(g.user_id)
    if sender["username"].lower() == to_username.lower():
        return jsonify({"ok": False, "error": "Нельзя переводить самому себе"}), 400

    receiver = get_user_by_username(to_username)
    if not receiver:
        return jsonify({"ok": False, "error": "Получатель не найден"}), 404

    db = get_db()
    transfer_amount_for_log: Any
    amount_label: str
    if currency == "RUB":
        if int(sender["balance"]) < int(amount_rub):
            return jsonify({"ok": False, "error": "Недостаточно RUB"}), 400
        db.execute("UPDATE users SET balance = balance - ? WHERE id = ?", (int(amount_rub), g.user_id))
        db.execute("UPDATE users SET balance = balance + ? WHERE id = ?", (int(amount_rub), int(receiver["id"])))
        transfer_amount_for_log = int(amount_rub)
        amount_label = f"{int(amount_rub)} RUB"
    else:
        sender_usd = decimal_two(sender["balance_usd"] if "balance_usd" in sender.keys() else 0)
        receiver_usd = decimal_two(receiver["balance_usd"] if "balance_usd" in receiver.keys() else 0)
        if sender_usd < amount_usd:
            return jsonify({"ok": False, "error": "Недостаточно USD"}), 400
        new_sender_usd = (sender_usd - amount_usd).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        new_receiver_usd = (receiver_usd + amount_usd).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        db.execute("UPDATE users SET balance_usd = ? WHERE id = ?", (float(new_sender_usd), g.user_id))
        db.execute("UPDATE users SET balance_usd = ? WHERE id = ?", (float(new_receiver_usd), int(receiver["id"])))
        transfer_amount_for_log = float(amount_usd)
        amount_label = f"{float(amount_usd):.2f} USD"
    db.commit()

    refresh_level(g.user_id)
    refresh_level(int(receiver["id"]))
    sender_updated = get_user_by_id(g.user_id)
    receiver_updated = get_user_by_id(int(receiver["id"]))

    log_action(
        g.user_id,
        "transfer_out",
        {
            "to_user_id": int(receiver["id"]),
            "to_username": receiver["username"],
            "amount": transfer_amount_for_log,
            "currency": currency,
            "balance_after": int(sender_updated["balance"]),
            "balance_after_usd": float(decimal_two(sender_updated["balance_usd"] if "balance_usd" in sender_updated.keys() else 0)),
        },
    )
    log_action(
        int(receiver["id"]),
        "transfer_in",
        {
            "from_user_id": g.user_id,
            "from_username": sender["username"],
            "amount": transfer_amount_for_log,
            "currency": currency,
            "balance_after": int(receiver_updated["balance"]),
            "balance_after_usd": float(decimal_two(receiver_updated["balance_usd"] if "balance_usd" in receiver_updated.keys() else 0)),
        },
    )
    add_notification(
        g.user_id,
        "transfer_out",
        "Исходящий перевод",
        f"Вы отправили {amount_label} пользователю {receiver['username']}",
        {
            "to_user_id": int(receiver["id"]),
            "to_username": receiver["username"],
            "amount": transfer_amount_for_log,
            "currency": currency,
        },
    )
    add_notification(
        int(receiver["id"]),
        "transfer_in",
        "Входящий перевод",
        f"Вы получили {amount_label} от {sender['username']}",
        {
            "from_user_id": g.user_id,
            "from_username": sender["username"],
            "amount": transfer_amount_for_log,
            "currency": currency,
        },
    )

    add_achievement(g.user_id, "first_transfer")
    transfer_rub_equivalent = int(amount_rub) if currency == "RUB" else int((amount_usd * Decimal(str(get_usd_rate(auto_update=False)["rate"]))).quantize(Decimal("1"), rounding=ROUND_DOWN))
    if transfer_rub_equivalent >= 200000:
        mark_suspicious(g.user_id, "large_transfer_sender")
        mark_suspicious(int(receiver["id"]), "large_transfer_receiver")

    return jsonify({"ok": True, "message": f"Перевод {amount_label} отправлен {receiver['username']}", "user": user_public_payload(sender_updated)})


@app.post("/api/game5050")
@require_auth
def api_game_5050():
    allowed, wait_for = check_cooldown(g.user_id, "game", GAME_COOLDOWN_SECONDS)
    if not allowed:
        log_action(g.user_id, "game_rate_limited", {"wait_for": wait_for})
        return jsonify({"ok": False, "error": f"Игра раз в 2с ({wait_for}с)"}), 429

    active_credit = enforce_overdue_penalty(g.user_id)
    if active_credit and credit_is_overdue(active_credit):
        return jsonify({"ok": False, "error": "Игры заблокированы: просроченный кредит"}), 403

    payload = request.get_json(silent=True) or {}
    bet = to_int(payload.get("bet"))
    if bet is None or bet <= 0:
        return jsonify({"ok": False, "error": "Ставка должна быть больше 0"}), 400

    user = get_user_by_id(g.user_id)
    if bet > int(user["balance"]):
        return jsonify({"ok": False, "error": "Недостаточно средств"}), 400

    vip_level = int(user["vip_level"])
    chance = VIP_CONFIG.get(vip_level, VIP_CONFIG[0])["win_chance"]
    roll = random.uniform(0, 100)
    won = roll <= chance

    db = get_db()
    if won:
        payout = bet * 2
        if vip_level >= 3:
            payout = int(payout * 1.15)
        delta = payout - bet
        db.execute("UPDATE users SET balance = balance + ? WHERE id = ?", (delta, g.user_id))
        action_name = "game_win"
        message = f"ПОБЕДА +{delta}"
        add_achievement(g.user_id, "first_win")
        apply_referral_earning_bonus(g.user_id, delta)
    else:
        delta = -bet
        db.execute("UPDATE users SET balance = balance - ? WHERE id = ?", (bet, g.user_id))
        action_name = "game_loss"
        message = "ПРОИГРЫШ"

    db.commit()
    refresh_level(g.user_id)
    updated = get_user_by_id(g.user_id)
    log_action(
        g.user_id,
        action_name,
        {
            "bet": bet,
            "chance": chance,
            "roll": round(roll, 2),
            "delta": delta,
            "balance_after": int(updated["balance"]),
        },
    )
    if won:
        add_notification(g.user_id, "game_win", "Игра 50/50", f"Победа! Изменение баланса: +{delta}", {"bet": bet, "delta": delta})
    else:
        add_notification(g.user_id, "game_loss", "Игра 50/50", f"Проигрыш. Изменение баланса: {delta}", {"bet": bet, "delta": delta})
    add_achievement(g.user_id, "first_game")

    return jsonify(
        {
            "ok": True,
            "won": won,
            "chance": chance,
            "roll": round(roll, 2),
            "delta": delta,
            "message": message,
            "user": user_public_payload(updated),
        }
    )


@app.post("/api/vip/upgrade")
@require_auth
def api_vip_upgrade():
    user = get_user_by_id(g.user_id)
    current = int(user["vip_level"])
    if current >= 4:
        return jsonify({"ok": False, "error": "VIP уже максимальный"}), 400

    next_level = current + 1
    cost = VIP_UPGRADE_COSTS[next_level]
    if int(user["balance"]) < cost:
        return jsonify({"ok": False, "error": f"Недостаточно средств. Нужно {cost}"}), 400

    db = get_db()
    db.execute("UPDATE users SET balance = balance - ?, vip_level = ? WHERE id = ?", (cost, next_level, g.user_id))
    db.commit()

    refresh_level(g.user_id)
    updated = get_user_by_id(g.user_id)
    add_achievement(g.user_id, "vip_purchase")
    log_action(
        g.user_id,
        "vip_upgrade",
        {"from_level": current, "to_level": next_level, "cost": cost, "balance_after": int(updated["balance"])},
    )
    add_notification(
        g.user_id,
        "vip_upgrade",
        "VIP повышен",
        f"Теперь ваш VIP: {VIP_CONFIG[next_level]['name']}. Списано {cost}.",
        {"from_level": current, "to_level": next_level, "cost": cost},
    )

    return jsonify({"ok": True, "message": f"VIP повышен до {VIP_CONFIG[next_level]['name']}", "cost": cost, "user": user_public_payload(updated)})


@app.get("/api/referrals")
@require_auth
def api_referrals():
    user = get_user_by_id(g.user_id)
    rows = get_db().execute(
        "SELECT id, username, balance, vip_level, level, last_active FROM users WHERE ref_by = ? ORDER BY id DESC",
        (g.user_id,),
    ).fetchall()

    referrals: List[Dict[str, Any]] = []
    for row in rows:
        vip_level = int(row["vip_level"])
        referrals.append(
            {
                "id": int(row["id"]),
                "username": row["username"],
                "balance": int(row["balance"]),
                "vip_level": vip_level,
                "vip_name": VIP_CONFIG.get(vip_level, VIP_CONFIG[0])["name"],
                "level": int(row["level"]),
                "last_active": row["last_active"],
            }
        )

    return jsonify(
        {
            "ok": True,
            "ref_code": user["ref_code"],
            "referral_earnings": int(user["referral_earnings"]),
            "referrals": referrals,
        }
    )


@app.get("/api/top/balance")
@require_auth
def api_top_balance():
    currency = str(request.args.get("currency", "RUB")).strip().upper()
    if currency not in {"RUB", "USD"}:
        return jsonify({"ok": False, "error": "Валюта должна быть RUB или USD"}), 400

    limit = to_int(request.args.get("limit"), 50)
    if limit is None:
        limit = 50
    limit = max(5, min(limit, 200))

    db = get_db()
    rate_info = get_usd_rate(auto_update=True)
    rate = Decimal(str(rate_info["rate"]))

    rows = db.execute(
        "SELECT id, username, full_name, balance, balance_usd, vip_level, level FROM users"
    ).fetchall()

    entries: List[Dict[str, Any]] = []
    for row in rows:
        balance_rub = int(row["balance"])
        balance_usd = decimal_two(row["balance_usd"] if "balance_usd" in row.keys() else 0)
        total_rub = Decimal(balance_rub) + (balance_usd * rate)
        if currency == "USD":
            display_value = (
                (total_rub / rate).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                if rate > 0
                else Decimal("0.00")
            )
        else:
            display_value = total_rub.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

        vip_level = int(row["vip_level"])
        entries.append(
            {
                "id": int(row["id"]),
                "username": row["username"],
                "full_name": row["full_name"] if "full_name" in row.keys() else row["username"],
                "vip_level": vip_level,
                "vip_name": VIP_CONFIG.get(vip_level, VIP_CONFIG[0])["name"],
                "vip_icon": VIP_CONFIG.get(vip_level, VIP_CONFIG[0])["icon"],
                "level": int(row["level"]),
                "balance_rub": balance_rub,
                "balance_usd": float(balance_usd),
                "display_balance": float(display_value),
                "sort_total_rub": float(total_rub),
            }
        )

    entries.sort(
        key=lambda x: (
            x["sort_total_rub"],
            x["balance_rub"],
            x["balance_usd"],
            -int(x["id"]),
        ),
        reverse=True,
    )

    me_rank = None
    for idx, item in enumerate(entries, start=1):
        if int(item["id"]) == g.user_id:
            me_rank = idx
            break

    top_payload: List[Dict[str, Any]] = []
    for idx, item in enumerate(entries[:limit], start=1):
        row_payload = {
            "rank": idx,
            "id": int(item["id"]),
            "username": item["username"],
            "full_name": item["full_name"],
            "vip_level": int(item["vip_level"]),
            "vip_name": item["vip_name"],
            "vip_icon": item["vip_icon"],
            "level": int(item["level"]),
            "balance_rub": int(item["balance_rub"]),
            "balance_usd": float(item["balance_usd"]),
            "display_balance": float(item["display_balance"]),
        }
        top_payload.append(row_payload)

    return jsonify(
        {
            "ok": True,
            "currency": currency,
            "usd_rate": float(rate),
            "usd_rate_updated_at": rate_info["updated_at"],
            "top": top_payload,
            "me_rank": me_rank,
            "total_users": len(entries),
        }
    )


@app.get("/api/stats")
@require_auth
def api_stats():
    rows = get_db().execute(
        "SELECT action, timestamp, info FROM logs WHERE user_id = ? ORDER BY id ASC",
        (g.user_id,),
    ).fetchall()

    today = now_utc().date()
    labels: List[str] = []
    activity_map: Dict[str, int] = {}
    for i in range(6, -1, -1):
        key = (today - timedelta(days=i)).isoformat()
        labels.append(key)
        activity_map[key] = 0

    wins = 0
    losses = 0
    balance_points: List[Dict[str, Any]] = []

    for row in rows:
        stamp = parse_iso(row["timestamp"])
        if stamp:
            day_key = stamp.date().isoformat()
            if day_key in activity_map:
                activity_map[day_key] += 1

        if row["action"] == "game_win":
            wins += 1
        elif row["action"] == "game_loss":
            losses += 1

        info = parse_json(row["info"], {})
        if isinstance(info, dict) and "balance_after" in info:
            balance_points.append({"timestamp": row["timestamp"], "balance": int(info["balance_after"])})

    return jsonify(
        {
            "ok": True,
            "activity": {"labels": labels, "values": [activity_map[k] for k in labels]},
            "balance_history": balance_points[-40:],
            "wins_losses": {"wins": wins, "losses": losses},
        }
    )


@app.get("/api/credits")
@require_auth
def api_credits():
    user = get_user_by_id(g.user_id)
    enforce_overdue_penalty(g.user_id)
    active = get_active_credit(g.user_id)

    rows = get_db().execute(
        "SELECT id, amount, total_to_pay, taken_at, due_at, repaid FROM credits WHERE user_id = ? ORDER BY id DESC LIMIT 10",
        (g.user_id,),
    ).fetchall()

    history = [
        {
            "id": int(row["id"]),
            "amount": int(row["amount"]),
            "total_to_pay": int(row["total_to_pay"]),
            "taken_at": row["taken_at"],
            "due_at": row["due_at"],
            "repaid": bool(row["repaid"]),
            "overdue": (not bool(row["repaid"])) and credit_is_overdue(row),
        }
        for row in rows
    ]

    active_payload = None
    if active:
        active_payload = {
            "id": int(active["id"]),
            "amount": int(active["amount"]),
            "total_to_pay": int(active["total_to_pay"]),
            "taken_at": active["taken_at"],
            "due_at": active["due_at"],
            "repaid": bool(active["repaid"]),
            "overdue": credit_is_overdue(active),
        }

    terms = get_credit_terms(int(user["vip_level"]), int(user["level"]))
    return jsonify({"ok": True, "terms": terms, "active": active_payload, "history": history})


@app.post("/api/credit/take")
@require_auth
def api_credit_take():
    payload = request.get_json(silent=True) or {}
    amount = to_int(payload.get("amount"))
    if amount is None or amount <= 0:
        return jsonify({"ok": False, "error": "Сумма должна быть больше 0"}), 400
    if get_active_credit(g.user_id):
        return jsonify({"ok": False, "error": "Сначала погасите активный кредит"}), 400

    user = get_user_by_id(g.user_id)
    terms = get_credit_terms(int(user["vip_level"]), int(user["level"]))
    if amount > int(terms["limit"]):
        return jsonify({"ok": False, "error": f"Лимит кредита: {terms['limit']}"}), 400

    total_to_pay = int(round(amount * (1 + terms["interest_percent"] / 100.0)))
    taken_at = now_utc()
    due_at = taken_at + timedelta(days=int(terms["deadline_days"]))

    db = get_db()
    db.execute(
        "INSERT INTO credits (user_id, amount, total_to_pay, taken_at, due_at, repaid) VALUES (?, ?, ?, ?, ?, 0)",
        (g.user_id, amount, total_to_pay, to_iso(taken_at), to_iso(due_at)),
    )
    db.execute("UPDATE users SET balance = balance + ? WHERE id = ?", (amount, g.user_id))
    db.commit()

    refresh_level(g.user_id)
    updated = get_user_by_id(g.user_id)
    log_action(
        g.user_id,
        "credit_taken",
        {
            "amount": amount,
            "total_to_pay": total_to_pay,
            "deadline_days": terms["deadline_days"],
            "balance_after": int(updated["balance"]),
        },
    )
    add_notification(
        g.user_id,
        "credit_taken",
        "Кредит оформлен",
        f"Получено {amount}. К возврату {total_to_pay}.",
        {"amount": amount, "total_to_pay": total_to_pay, "due_at": to_iso(due_at)},
    )

    return jsonify({"ok": True, "message": f"Кредит одобрен: +{amount}", "user": user_public_payload(updated)})


@app.post("/api/credit/repay")
@require_auth
def api_credit_repay():
    enforce_overdue_penalty(g.user_id)
    credit = get_active_credit(g.user_id)
    if not credit:
        return jsonify({"ok": False, "error": "Нет активного кредита"}), 400

    user = get_user_by_id(g.user_id)
    to_pay = int(credit["total_to_pay"])
    if int(user["balance"]) < to_pay:
        return jsonify({"ok": False, "error": f"Недостаточно средств ({to_pay})"}), 400

    db = get_db()
    db.execute("UPDATE users SET balance = balance - ? WHERE id = ?", (to_pay, g.user_id))
    db.execute("UPDATE credits SET repaid = 1 WHERE id = ?", (int(credit["id"]),))
    db.commit()

    refresh_level(g.user_id)
    updated = get_user_by_id(g.user_id)
    log_action(g.user_id, "credit_repaid", {"credit_id": int(credit["id"]), "amount": to_pay, "balance_after": int(updated["balance"])})
    add_notification(
        g.user_id,
        "credit_repaid",
        "Кредит погашен",
        f"Вы погасили кредит на сумму {to_pay}.",
        {"credit_id": int(credit["id"]), "amount": to_pay},
    )

    return jsonify({"ok": True, "message": f"Кредит погашен: -{to_pay}", "user": user_public_payload(updated)})

@app.post("/api/admin/login")
def api_admin_login():
    payload = request.get_json(silent=True) or {}
    password = str(payload.get("password", "")).strip()
    new_password = str(payload.get("new_password", "")).strip()
    confirm_password = str(payload.get("confirm_password", "")).strip()
    user_id = session.get("user_id")
    user = get_user_by_id(int(user_id)) if user_id else None
    if not is_admin_user(user):
        log_action(user_id, "admin_login_denied", {"reason": "admin_level_zero"})
        return jsonify({"ok": False, "error": "У вас нет прав админа"}), 403

    stored_hash = str(user["admin_panel_password_hash"] or "") if "admin_panel_password_hash" in user.keys() else ""
    password_is_set = bool(stored_hash.strip())
    if not password_is_set:
        if not new_password:
            return jsonify(
                {
                    "ok": False,
                    "error": "Для первого входа установите пароль админки",
                    "need_set_password": True,
                }
            ), 400
        if len(new_password) < 6:
            return jsonify({"ok": False, "error": "Пароль админки минимум 6 символов", "need_set_password": True}), 400
        if new_password != confirm_password:
            return jsonify({"ok": False, "error": "Пароль админки и подтверждение не совпадают", "need_set_password": True}), 400
        new_hash = hash_password(new_password)
        db = get_db()
        db.execute("UPDATE users SET admin_panel_password_hash = ? WHERE id = ?", (new_hash, int(user["id"])))
        db.commit()
        log_action(session.get("user_id"), "admin_password_set", {})
    else:
        if len(password) < 1:
            return jsonify({"ok": False, "error": "Введите пароль админки"}), 400
        if hash_password(password) != stored_hash:
            log_action(session.get("user_id"), "admin_login_failed", {})
            return jsonify({"ok": False, "error": "Неверный пароль админки"}), 401

    session["is_admin"] = True
    updated_user = get_user_by_id(int(user["id"]))
    session["admin_level"] = int(updated_user["admin_level"])
    log_action(session.get("user_id"), "admin_login_success", {})
    return jsonify({"ok": True, "message": "Админ-доступ открыт", "admin_level": int(updated_user["admin_level"]), "password_set": True})


@app.post("/api/admin/logout")
def api_admin_logout():
    if session.get("is_admin"):
        log_action(session.get("user_id"), "admin_logout", {})
    session["is_admin"] = False
    session["admin_level"] = 0
    return jsonify({"ok": True})


@app.post("/api/admin/grant_level")
@require_admin
def api_admin_grant_level():
    denied = admin_level_guard(6)
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    username = str(payload.get("username", "")).strip()
    new_admin_level = to_int(payload.get("admin_level"))
    if not username:
        return jsonify({"ok": False, "error": "Укажите ник"}), 400
    if new_admin_level is None or new_admin_level < 0 or new_admin_level > 7:
        return jsonify({"ok": False, "error": "Уровень админки должен быть в диапазоне 0-7"}), 400

    max_grant = int(g.admin_caps["max_assign_admin_level"])
    if max_grant <= 0:
        return jsonify({"ok": False, "error": "Ваш уровень админки не может выдавать админ-права"}), 403
    if new_admin_level > max_grant:
        return jsonify({"ok": False, "error": f"Вы можете выдавать максимум уровень {max_grant}"}), 403

    target = get_user_by_username(username)
    if not target:
        return jsonify({"ok": False, "error": "Пользователь не найден"}), 404
    if is_root_user(target) and not bool(getattr(g, "is_root_admin", False)):
        return jsonify({"ok": False, "error": f"Пользователь {ROOT_USERNAME} имеет root-иммунитет"}), 403
    if target["username"].lower() == "admin" and new_admin_level < 6:
        return jsonify({"ok": False, "error": "Пользователь admin всегда имеет уровень 6"}), 400
    if is_root_user(target) and new_admin_level < 7:
        return jsonify({"ok": False, "error": f"Пользователь {ROOT_USERNAME} всегда имеет root-доступ"}), 400

    db = get_db()
    db.execute("UPDATE users SET admin_level = ? WHERE id = ?", (new_admin_level, int(target["id"])))
    db.commit()
    log_action(
        int(target["id"]),
        "admin_level_granted",
        {
            "by_user_id": session.get("user_id"),
            "new_admin_level": new_admin_level,
        },
    )
    add_notification(
        int(target["id"]),
        "admin_level",
        "Изменён уровень админки",
        f"Ваш новый уровень админки: {new_admin_level} ({admin_level_name(new_admin_level)}).",
        {"new_admin_level": new_admin_level, "by_user_id": session.get("user_id")},
    )
    return jsonify({"ok": True, "message": f"Пользователю {target['username']} выдан уровень админки {new_admin_level}"})


@app.post("/api/admin/user/warn")
@require_admin
def api_admin_warn_user():
    denied = admin_level_guard(3)
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    user_id = to_int(payload.get("user_id"))
    reason = str(payload.get("reason", "")).strip() or "Без причины"
    if user_id is None:
        return jsonify({"ok": False, "error": "Некорректный user_id"}), 400
    target = get_user_by_id(user_id)
    if not target:
        return jsonify({"ok": False, "error": "Пользователь не найден"}), 404
    denied = guard_admin_hierarchy_for_moderation(target, "выдавать предупреждение")
    if denied:
        return denied

    warnings = int(target["warnings"]) + 1
    db = get_db()
    auto_ban_until = None
    if warnings >= 3:
        warnings = 0
        auto_ban_until = now_utc() + timedelta(days=5)
        db.execute("UPDATE users SET warnings = ?, banned_until = ? WHERE id = ?", (warnings, to_iso(auto_ban_until), user_id))
    else:
        db.execute("UPDATE users SET warnings = ? WHERE id = ?", (warnings, user_id))
    db.commit()

    log_action(
        user_id,
        "admin_warn",
        {
            "by_user_id": session.get("user_id"),
            "reason": reason,
            "warnings_after": warnings,
            "auto_ban_until": to_iso(auto_ban_until) if auto_ban_until else None,
        },
    )
    if auto_ban_until:
        add_notification(
            user_id,
            "warn",
            "Предупреждение и авто-бан",
            f"Получено 3 предупреждения. Аккаунт заблокирован до {to_iso(auto_ban_until)}.",
            {"reason": reason, "banned_until": to_iso(auto_ban_until)},
        )
    else:
        add_notification(
            user_id,
            "warn",
            "Предупреждение",
            f"Администратор выдал предупреждение. Причина: {reason}",
            {"reason": reason, "warnings_after": warnings},
        )
    if auto_ban_until:
        return jsonify({"ok": True, "message": f"Выдано предупреждение. Накопилось 3 предупреждения: авто-бан до {to_iso(auto_ban_until)}"})
    return jsonify({"ok": True, "message": f"Предупреждение выдано. Теперь предупреждений: {warnings}"})


@app.post("/api/admin/user/clear_warnings")
@require_admin
def api_admin_clear_warnings():
    denied = admin_level_guard(3)
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    user_id = to_int(payload.get("user_id"))
    if user_id is None:
        return jsonify({"ok": False, "error": "Некорректный user_id"}), 400
    target = get_user_by_id(user_id)
    if not target:
        return jsonify({"ok": False, "error": "Пользователь не найден"}), 404
    denied = guard_admin_hierarchy_for_moderation(target, "снимать предупреждения")
    if denied:
        return denied

    db = get_db()
    db.execute("UPDATE users SET warnings = 0 WHERE id = ?", (user_id,))
    db.commit()
    log_action(
        user_id,
        "admin_clear_warnings",
        {"by_user_id": session.get("user_id")},
    )
    add_notification(
        user_id,
        "warn_clear",
        "Предупреждения сняты",
        "Администратор снял все предупреждения с вашего аккаунта.",
        {"by_user_id": session.get("user_id")},
    )
    return jsonify({"ok": True, "message": "Предупреждения сняты"})


@app.post("/api/admin/user/ban")
@require_admin
def api_admin_ban_user():
    denied = admin_level_guard(3)
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    user_id = to_int(payload.get("user_id"))
    days_raw = payload.get("days")
    days = to_int(days_raw)
    reason = str(payload.get("reason", "")).strip() or "Без причины"
    permanent = bool(payload.get("permanent"))
    if isinstance(days_raw, str) and days_raw.strip().lower() in {"perm", "permanent", "forever", "навсегда"}:
        permanent = True
    if user_id is None:
        return jsonify({"ok": False, "error": "Укажите корректный user_id"}), 400
    if not permanent and (days is None or days <= 0):
        return jsonify({"ok": False, "error": "Укажите корректные user_id и days"}), 400

    max_days = int(g.admin_caps["max_ban_days"])
    if permanent and int(g.admin_level) < 6:
        return jsonify({"ok": False, "error": "Бан навсегда доступен только уровню админки 6"}), 403
    if not permanent and days > max_days:
        return jsonify({"ok": False, "error": f"Ваш уровень админки может банить максимум на {max_days} дней"}), 403

    target = get_user_by_id(user_id)
    if not target:
        return jsonify({"ok": False, "error": "Пользователь не найден"}), 404
    denied = guard_admin_hierarchy_for_moderation(target, "банить")
    if denied:
        return denied
    if target["username"].lower() == "admin" and not bool(getattr(g, "is_root_admin", False)):
        return jsonify({"ok": False, "error": "Пользователя admin банить нельзя"}), 400

    if permanent:
        banned_until = "PERMANENT"
    else:
        banned_until = to_iso(now_utc() + timedelta(days=days))
    db = get_db()
    db.execute("UPDATE users SET banned_until = ?, warnings = 0 WHERE id = ?", (banned_until, user_id))
    db.commit()
    log_action(
        user_id,
        "admin_ban",
        {
            "by_user_id": session.get("user_id"),
            "days": None if permanent else days,
            "permanent": permanent,
            "reason": reason,
            "banned_until": banned_until,
        },
    )
    if permanent:
        add_notification(
            user_id,
            "ban",
            "Аккаунт заблокирован",
            f"Аккаунт заблокирован навсегда. Причина: {reason}",
            {"permanent": True, "reason": reason},
        )
        return jsonify({"ok": True, "message": "Пользователь забанен навсегда"})
    add_notification(
        user_id,
        "ban",
        "Аккаунт заблокирован",
        f"Блокировка до {banned_until}. Причина: {reason}",
        {"days": days, "reason": reason, "banned_until": banned_until},
    )
    return jsonify({"ok": True, "message": f"Пользователь забанен до {banned_until}"})


@app.post("/api/admin/user/unban")
@require_admin
def api_admin_unban_user():
    denied = admin_level_guard(3)
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    user_id = to_int(payload.get("user_id"))
    if user_id is None:
        return jsonify({"ok": False, "error": "Некорректный user_id"}), 400
    target = get_user_by_id(user_id)
    if not target:
        return jsonify({"ok": False, "error": "Пользователь не найден"}), 404
    denied = guard_admin_hierarchy_for_moderation(target, "разбанивать")
    if denied:
        return denied

    db = get_db()
    db.execute("UPDATE users SET banned_until = NULL, warnings = 0 WHERE id = ?", (user_id,))
    db.commit()
    log_action(user_id, "admin_unban", {"by_user_id": session.get("user_id")})
    add_notification(
        user_id,
        "unban",
        "Блокировка снята",
        "Ваш аккаунт разблокирован администратором.",
        {"by_user_id": session.get("user_id")},
    )
    return jsonify({"ok": True, "message": "Пользователь разблокирован"})


@app.get("/api/admin/support/conversations")
@require_admin
def api_admin_support_conversations():
    denied = admin_level_guard(2)
    if denied:
        return denied
    db = get_db()
    admin_user = get_user_by_id(int(session.get("user_id")))
    last_seen = admin_user["support_last_seen_admin"] or "1970-01-01T00:00:00+00:00"
    rows = db.execute(
        """
        SELECT
            u.id AS user_id,
            u.username,
            u.full_name,
            MAX(s.timestamp) AS last_timestamp,
            SUM(CASE WHEN s.sender_role = 'user' AND s.timestamp > ? THEN 1 ELSE 0 END) AS unread_user_messages
        FROM support_messages s
        JOIN users u ON u.id = s.user_id
        GROUP BY u.id
        ORDER BY last_timestamp DESC
        """,
        (last_seen,),
    ).fetchall()
    conversations = [
        {
            "user_id": int(row["user_id"]),
            "username": row["username"],
            "full_name": row["full_name"],
            "last_timestamp": row["last_timestamp"],
            "unread_user_messages": int(row["unread_user_messages"] or 0),
        }
        for row in rows
    ]
    db.execute("UPDATE users SET support_last_seen_admin = ? WHERE id = ?", (to_iso(now_utc()), int(session.get("user_id"))))
    db.commit()
    return jsonify({"ok": True, "conversations": conversations})


@app.get("/api/admin/support/messages/<int:user_id>")
@require_admin
def api_admin_support_messages(user_id: int):
    denied = admin_level_guard(2)
    if denied:
        return denied
    user = get_user_by_id(user_id)
    if not user:
        return jsonify({"ok": False, "error": "Пользователь не найден"}), 404
    rows = get_db().execute(
        """
        SELECT s.id, s.sender_role, s.message, s.timestamp, u.username
        FROM support_messages s
        JOIN users u ON u.id = s.sender_user_id
        WHERE s.user_id = ?
        ORDER BY s.id ASC
        """,
        (user_id,),
    ).fetchall()
    messages = [
        {
            "id": int(row["id"]),
            "sender_role": row["sender_role"],
            "sender_username": row["username"],
            "message": row["message"],
            "timestamp": row["timestamp"],
        }
        for row in rows
    ]
    db = get_db()
    db.execute("UPDATE users SET support_last_seen_admin = ? WHERE id = ?", (to_iso(now_utc()), int(session.get("user_id"))))
    db.commit()
    return jsonify({"ok": True, "messages": messages, "target_user": {"id": int(user["id"]), "username": user["username"], "full_name": user["full_name"]}})


@app.post("/api/admin/support/reply")
@require_admin
def api_admin_support_reply():
    denied = admin_level_guard(2)
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    user_id = to_int(payload.get("user_id"))
    message = str(payload.get("message", "")).strip()
    if user_id is None or not message:
        return jsonify({"ok": False, "error": "Укажите user_id и message"}), 400
    target = get_user_by_id(user_id)
    if not target:
        return jsonify({"ok": False, "error": "Пользователь не найден"}), 404

    db = get_db()
    db.execute(
        """
        INSERT INTO support_messages (user_id, sender_user_id, sender_role, message, timestamp)
        VALUES (?, ?, 'admin', ?, ?)
        """,
        (user_id, int(session.get("user_id")), message, to_iso(now_utc())),
    )
    db.commit()
    log_action(user_id, "support_reply_sent", {"by_admin_id": session.get("user_id"), "length": len(message)})
    add_notification(
        user_id,
        "support_reply",
        "Ответ поддержки",
        "Служба поддержки ответила на ваше обращение.",
        {"length": len(message)},
    )
    return jsonify({"ok": True, "message": "Ответ отправлен"})


@app.get("/api/admin/chat/messages")
@require_admin
def api_admin_chat_messages():
    denied = admin_level_guard(2)
    if denied:
        return denied
    db = get_db()
    me_id = int(session.get("user_id"))
    me = get_user_by_id(me_id)
    last_seen = me["admin_chat_last_seen"] or "1970-01-01T00:00:00+00:00"
    unread_row = db.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM admin_chat_messages
        WHERE timestamp > ? AND sender_user_id != ?
        """,
        (last_seen, me_id),
    ).fetchone()
    unread = int(unread_row["cnt"]) if unread_row else 0

    rows = db.execute(
        """
        SELECT a.id, a.message, a.timestamp, u.id AS sender_id, u.username, u.admin_level
        FROM admin_chat_messages a
        JOIN users u ON u.id = a.sender_user_id
        ORDER BY a.id DESC
        LIMIT 200
        """
    ).fetchall()
    messages = [
        {
            "id": int(row["id"]),
            "message": row["message"],
            "timestamp": row["timestamp"],
            "sender_id": int(row["sender_id"]),
            "sender_username": row["username"],
            "sender_admin_level": int(row["admin_level"]),
        }
        for row in reversed(rows)
    ]
    db.execute("UPDATE users SET admin_chat_last_seen = ? WHERE id = ?", (to_iso(now_utc()), me_id))
    db.commit()
    return jsonify({"ok": True, "messages": messages, "unread_count": unread})


@app.post("/api/admin/chat/send")
@require_admin
def api_admin_chat_send():
    denied = admin_level_guard(2)
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    message = str(payload.get("message", "")).strip()
    if len(message) < 1 or len(message) > 1000:
        return jsonify({"ok": False, "error": "Сообщение должно быть от 1 до 1000 символов"}), 400

    db = get_db()
    db.execute(
        "INSERT INTO admin_chat_messages (sender_user_id, message, timestamp) VALUES (?, ?, ?)",
        (int(session.get("user_id")), message, to_iso(now_utc())),
    )
    db.commit()
    return jsonify({"ok": True, "message": "Сообщение отправлено"})


@app.get("/api/admin/ngrok/status")
@app.get("/api/admin/serveo/status")
@app.get("/api/admin/cloudflare/status")
@require_admin
def api_admin_ngrok_status():
    denied = admin_level_guard(7)
    if denied:
        return denied
    status = get_ngrok_status()
    err_text = str(status.get("error") or "").lower()
    should_self_heal = bool(
        status.get("running")
        and not status.get("public_url")
        and any(token in err_text for token in ("устаревш", "legacy", "ngrok", "serveo"))
    )
    if should_self_heal:
        stop_ngrok_tunnel()
        status = start_ngrok_tunnel()
        if status.get("running") and status.get("public_url"):
            status["error"] = None
    return jsonify({"ok": True, "status": status})


@app.post("/api/admin/ngrok/provider")
@app.post("/api/admin/cloudflare/provider")
@require_admin
def api_admin_ngrok_set_provider():
    global NGROK_PREFERRED_PROVIDER
    denied = admin_level_guard(7)
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    provider = normalize_tunnel_provider(payload.get("provider"))
    with NGROK_LOCK:
        NGROK_PREFERRED_PROVIDER = provider
    status = get_ngrok_status()
    log_action(session.get("user_id"), "admin_tunnel_provider_set", {"provider": provider})
    return jsonify(
        {
            "ok": True,
            "message": "Провайдер туннеля обновлен",
            "preferred_provider": provider,
            "provider_options": status.get("provider_options", []),
            "status": status,
        }
    )


@app.post("/api/admin/ngrok/start")
@app.post("/api/admin/serveo/start")
@app.post("/api/admin/cloudflare/start")
@require_admin
def api_admin_ngrok_start():
    denied = admin_level_guard(7)
    if denied:
        return denied
    status = start_ngrok_tunnel()
    if not status["available"]:
        return jsonify({"ok": False, "error": status["error"], "status": status}), 400
    if not status["running"]:
        return jsonify({"ok": False, "error": status["error"] or "Не удалось запустить туннель", "status": status}), 500
    log_action(session.get("user_id"), "admin_tunnel_start", {"provider": status.get("provider"), "public_url": status["public_url"], "pid": status["pid"]})
    provider_name_map = {
        "cloudflare_quick_tunnel": "Cloudflare Tunnel",
        "pinggy": "Pinggy Tunnel",
        "localhost_run": "localhost.run Tunnel",
    }
    provider_label = provider_name_map.get(str(status.get("provider") or "").lower(), "Tunnel")
    return jsonify({"ok": True, "message": f"{provider_label} запущен", "status": status})


@app.post("/api/admin/ngrok/stop")
@app.post("/api/admin/serveo/stop")
@app.post("/api/admin/cloudflare/stop")
@require_admin
def api_admin_ngrok_stop():
    denied = admin_level_guard(7)
    if denied:
        return denied
    status = stop_ngrok_tunnel()
    log_action(session.get("user_id"), "admin_tunnel_stop", {"provider": status.get("provider")})
    return jsonify({"ok": True, "message": "Публичный туннель остановлен", "status": status})


@app.get("/api/admin/economy")
@require_admin
def api_admin_economy():
    denied = admin_level_guard(6)
    if denied:
        return denied
    db = get_db()
    rate_info = get_usd_rate(auto_update=True)
    limits = get_card_limits_config()
    rows = db.execute(
        """
        SELECT c.id, c.user_id, c.card_number, c.created_at, u.username
        FROM cards c
        JOIN users u ON u.id = c.user_id
        ORDER BY c.id DESC
        LIMIT 400
        """
    ).fetchall()
    cards = [
        {
            "id": int(row["id"]),
            "user_id": int(row["user_id"]),
            "username": row["username"],
            "card_number": row["card_number"],
            "created_at": row["created_at"],
        }
        for row in rows
    ]
    return jsonify(
        {
            "ok": True,
            "usd_rate": float(rate_info["rate"]),
            "usd_rate_updated_at": rate_info["updated_at"],
            "card_limits_by_vip": limits,
            "cards": cards,
        }
    )


@app.post("/api/admin/economy/usd_rate")
@require_admin
def api_admin_set_usd_rate():
    denied = admin_level_guard(6)
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    new_rate = parse_rate_value(payload.get("usd_rate"))
    if new_rate is None:
        return jsonify({"ok": False, "error": f"Курс USD должен быть в диапазоне {USD_RATE_MIN}..{USD_RATE_MAX}"}), 400
    updated = set_usd_rate_manual(new_rate)
    log_action(
        int(session.get("user_id")),
        "admin_usd_rate_set",
        {"usd_rate": float(updated["rate"])},
    )
    return jsonify({"ok": True, "message": f"Курс USD установлен: {float(updated['rate']):.4f}", "usd_rate": float(updated["rate"]), "usd_rate_updated_at": updated["updated_at"]})


@app.post("/api/admin/economy/card_limits")
@require_admin
def api_admin_set_card_limits():
    denied = admin_level_guard(6)
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    limits_payload = payload.get("limits")
    if not isinstance(limits_payload, dict):
        return jsonify({"ok": False, "error": "Передайте limits объектом"}), 400
    updated = set_card_limits_config(limits_payload)
    log_action(
        int(session.get("user_id")),
        "admin_card_limits_set",
        {"limits": updated},
    )
    return jsonify({"ok": True, "message": "Лимиты карт обновлены", "card_limits_by_vip": updated})


@app.post("/api/admin/economy/card/delete")
@require_admin
def api_admin_delete_card():
    denied = admin_level_guard(6)
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    card_id = to_int(payload.get("card_id"))
    if card_id is None:
        return jsonify({"ok": False, "error": "Некорректный card_id"}), 400
    db = get_db()
    card = db.execute("SELECT id, user_id, card_number FROM cards WHERE id = ?", (card_id,)).fetchone()
    if not card:
        return jsonify({"ok": False, "error": "Карта не найдена"}), 404
    db.execute("DELETE FROM cards WHERE id = ?", (card_id,))
    db.commit()
    log_action(
        int(session.get("user_id")),
        "admin_card_deleted",
        {"card_id": int(card["id"]), "user_id": int(card["user_id"]), "card_number": card["card_number"]},
    )
    add_notification(
        int(card["user_id"]),
        "admin_card_deleted",
        "Карта удалена администратором",
        f"Карта {card['card_number']} была удалена администратором.",
        {"card_id": int(card["id"]), "admin_user_id": session.get("user_id")},
    )
    return jsonify({"ok": True, "message": "Карта удалена"})


@app.get("/api/admin/overview")
@require_admin
def api_admin_overview():
    db = get_db()
    users_rows = db.execute(
        """
        SELECT id, username, full_name, balance, balance_usd, vip_level, level, suspicious, last_active, referral_earnings,
               admin_level, warnings, banned_until
        FROM users
        ORDER BY id DESC
        """
    ).fetchall()

    threshold = now_utc() - timedelta(seconds=60)
    users: List[Dict[str, Any]] = []
    admins: List[Dict[str, Any]] = []
    for row in users_rows:
        active = parse_iso(row["last_active"])
        ban_status = get_ban_status(row)
        row_payload = {
            "id": int(row["id"]),
            "username": row["username"],
            "full_name": row["full_name"],
            "balance": int(row["balance"]),
            "balance_usd": float(decimal_two(row["balance_usd"] if "balance_usd" in row.keys() else 0)),
            "vip_level": int(row["vip_level"]),
            "vip_name": VIP_CONFIG.get(int(row["vip_level"]), VIP_CONFIG[0])["name"],
            "level": int(row["level"]),
            "suspicious": bool(row["suspicious"]),
            "online": bool(active and active >= threshold),
            "last_active": row["last_active"],
            "referral_earnings": int(row["referral_earnings"]),
            "admin_level": int(row["admin_level"]),
            "admin_level_name": admin_level_name(int(row["admin_level"])),
            "warnings": int(row["warnings"]),
            "is_banned": ban_status["is_banned"],
            "banned_until": ban_status["banned_until"],
        }
        users.append(row_payload)
        if int(row["admin_level"]) > 0:
            admins.append(
                {
                    "id": int(row["id"]),
                    "username": row["username"],
                    "full_name": row["full_name"],
                    "admin_level": int(row["admin_level"]),
                    "admin_level_name": admin_level_name(int(row["admin_level"])),
                    "online": row_payload["online"],
                }
            )

    credits_rows = db.execute(
        """
        SELECT c.id, c.user_id, u.username, c.amount, c.total_to_pay, c.taken_at, c.due_at, c.repaid
        FROM credits c
        JOIN users u ON u.id = c.user_id
        WHERE c.repaid = 0
        ORDER BY c.due_at ASC
        """
    ).fetchall()

    credits: List[Dict[str, Any]] = []
    overdue: List[Dict[str, Any]] = []
    for row in credits_rows:
        entry = {
            "id": int(row["id"]),
            "user_id": int(row["user_id"]),
            "username": row["username"],
            "amount": int(row["amount"]),
            "total_to_pay": int(row["total_to_pay"]),
            "taken_at": row["taken_at"],
            "due_at": row["due_at"],
            "repaid": bool(row["repaid"]),
            "overdue": credit_is_overdue(row),
        }
        credits.append(entry)
        if entry["overdue"]:
            overdue.append(entry)

    suspicious_users = [u for u in users if u["suspicious"]]
    online_users = len([u for u in users if u["online"]])
    me_id = int(session.get("user_id"))
    me = get_user_by_id(me_id)
    support_last_seen = me["support_last_seen_admin"] or "1970-01-01T00:00:00+00:00"
    support_unread_row = db.execute(
        "SELECT COUNT(*) AS cnt FROM support_messages WHERE sender_role = 'user' AND timestamp > ?",
        (support_last_seen,),
    ).fetchone()
    support_unread = int(support_unread_row["cnt"]) if support_unread_row else 0

    chat_last_seen = me["admin_chat_last_seen"] or "1970-01-01T00:00:00+00:00"
    chat_unread_row = db.execute(
        "SELECT COUNT(*) AS cnt FROM admin_chat_messages WHERE timestamp > ? AND sender_user_id != ?",
        (chat_last_seen, me_id),
    ).fetchone()
    admin_chat_unread = int(chat_unread_row["cnt"]) if chat_unread_row else 0
    usd_rate_info = get_usd_rate(auto_update=True)
    card_limits = get_card_limits_config()

    return jsonify(
        {
            "ok": True,
            "admin_level": int(me["admin_level"]),
            "admin_level_name": admin_level_name(int(me["admin_level"])),
            "admin_caps": admin_capabilities(int(me["admin_level"]), is_root_user(me)),
            "usd_rate": float(usd_rate_info["rate"]),
            "usd_rate_updated_at": usd_rate_info["updated_at"],
            "card_limits_by_vip": card_limits,
            "admin_levels": ADMIN_LEVEL_NAMES,
            "online_users": online_users,
            "users": users,
            "admins": admins,
            "credits": credits,
            "overdue_credits": overdue,
            "suspicious_users": suspicious_users,
            "support_unread": support_unread,
            "admin_chat_unread": admin_chat_unread,
        }
    )


@app.get("/api/admin/logs")
@require_admin
def api_admin_logs():
    denied = admin_level_guard(4)
    if denied:
        return denied
    limit = to_int(request.args.get("limit"), 150)
    if limit is None:
        limit = 150
    limit = max(1, min(limit, 500))

    rows = get_db().execute(
        """
        SELECT l.id, l.user_id, u.username, l.action, l.timestamp, l.info
        FROM logs l
        LEFT JOIN users u ON u.id = l.user_id
        ORDER BY l.id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    return jsonify(
        {
            "ok": True,
            "logs": [
                {
                    "id": int(row["id"]),
                    "user_id": row["user_id"],
                    "username": row["username"],
                    "action": row["action"],
                    "timestamp": row["timestamp"],
                    "info": parse_json(row["info"], {}),
                }
                for row in rows
            ],
        }
    )


@app.post("/api/admin/user/balance")
@require_admin
def api_admin_set_balance():
    denied = admin_level_guard(5)
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    user_id = to_int(payload.get("user_id"))
    currency = str(payload.get("currency", "RUB")).strip().upper()
    new_balance_rub = to_int(payload.get("balance")) if currency == "RUB" else None
    new_balance_usd = None
    if currency == "USD":
        try:
            parsed_usd = Decimal(str(payload.get("balance"))).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        except (InvalidOperation, ValueError, TypeError):
            parsed_usd = Decimal("-1")
        if parsed_usd >= Decimal("0.00"):
            new_balance_usd = parsed_usd
    if currency not in {"RUB", "USD"}:
        return jsonify({"ok": False, "error": "Валюта должна быть RUB или USD"}), 400
    if user_id is None:
        return jsonify({"ok": False, "error": "Некорректные параметры"}), 400
    if currency == "RUB" and (new_balance_rub is None or new_balance_rub < 0):
        return jsonify({"ok": False, "error": "Некорректный RUB баланс"}), 400
    if currency == "USD" and new_balance_usd is None:
        return jsonify({"ok": False, "error": "Некорректный USD баланс"}), 400
    target = get_user_by_id(user_id)
    if not target:
        return jsonify({"ok": False, "error": "Пользователь не найден"}), 404
    denied = guard_root_immunity(target, "изменять баланс")
    if denied:
        return denied

    db = get_db()
    if currency == "RUB":
        db.execute("UPDATE users SET balance = ? WHERE id = ?", (int(new_balance_rub), user_id))
    else:
        db.execute("UPDATE users SET balance_usd = ? WHERE id = ?", (float(new_balance_usd), user_id))
    db.commit()
    refresh_level(user_id)
    log_action(
        user_id,
        "admin_balance_set",
        {
            "admin_user_id": session.get("user_id"),
            "currency": currency,
            "new_balance": int(new_balance_rub) if currency == "RUB" else float(new_balance_usd),
        },
    )
    new_balance_label = f"{int(new_balance_rub)} RUB" if currency == "RUB" else f"{float(new_balance_usd):.2f} USD"
    add_notification(
        user_id,
        "admin_balance",
        "Баланс изменён администратором",
        f"Ваш баланс установлен в {new_balance_label}.",
        {
            "currency": currency,
            "new_balance": int(new_balance_rub) if currency == "RUB" else float(new_balance_usd),
            "admin_user_id": session.get("user_id"),
        },
    )
    return jsonify({"ok": True, "message": "Баланс обновлён"})


@app.post("/api/admin/user/vip")
@require_admin
def api_admin_set_vip():
    denied = admin_level_guard(5)
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    user_id = to_int(payload.get("user_id"))
    vip_level = to_int(payload.get("vip_level"))
    if user_id is None or vip_level is None or vip_level < 0 or vip_level > 4:
        return jsonify({"ok": False, "error": "Некорректные параметры"}), 400
    target = get_user_by_id(user_id)
    if not target:
        return jsonify({"ok": False, "error": "Пользователь не найден"}), 404
    denied = guard_root_immunity(target, "изменять VIP")
    if denied:
        return denied

    db = get_db()
    db.execute("UPDATE users SET vip_level = ? WHERE id = ?", (vip_level, user_id))
    db.commit()
    log_action(user_id, "admin_vip_set", {"admin_user_id": session.get("user_id"), "new_vip_level": vip_level})
    add_notification(
        user_id,
        "admin_vip",
        "VIP изменён администратором",
        f"Ваш VIP установлен на {vip_level} ({VIP_CONFIG.get(vip_level, VIP_CONFIG[0])['name']}).",
        {"vip_level": vip_level, "admin_user_id": session.get("user_id")},
    )
    return jsonify({"ok": True, "message": "VIP обновлён"})


@app.post("/api/admin/user/suspicious")
@require_admin
def api_admin_set_suspicious():
    denied = admin_level_guard(5)
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    user_id = to_int(payload.get("user_id"))
    suspicious = 1 if payload.get("suspicious") else 0
    if user_id is None:
        return jsonify({"ok": False, "error": "Некорректный user_id"}), 400
    target = get_user_by_id(user_id)
    if not target:
        return jsonify({"ok": False, "error": "Пользователь не найден"}), 404
    denied = guard_root_immunity(target, "изменять suspicious-статус")
    if denied:
        return denied

    db = get_db()
    db.execute("UPDATE users SET suspicious = ? WHERE id = ?", (suspicious, user_id))
    db.commit()
    log_action(user_id, "admin_suspicious_set", {"admin_user_id": session.get("user_id"), "suspicious": suspicious})
    add_notification(
        user_id,
        "admin_suspicious",
        "Изменён статус suspicious",
        f"Статус suspicious: {'Да' if suspicious else 'Нет'}.",
        {"suspicious": suspicious, "admin_user_id": session.get("user_id")},
    )
    return jsonify({"ok": True, "message": "Статус suspicious обновлён"})


@app.post("/api/admin/user/level")
@require_admin
def api_admin_set_level():
    denied = admin_level_guard(5)
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    user_id = to_int(payload.get("user_id"))
    level = to_int(payload.get("level"))
    if user_id is None or level is None or level < 1 or level > 100:
        return jsonify({"ok": False, "error": "Уровень должен быть в диапазоне 1-100"}), 400
    target = get_user_by_id(user_id)
    if not target:
        return jsonify({"ok": False, "error": "Пользователь не найден"}), 404
    denied = guard_root_immunity(target, "изменять уровень")
    if denied:
        return denied

    db = get_db()
    db.execute("UPDATE users SET level = ? WHERE id = ?", (level, user_id))
    db.commit()
    log_action(user_id, "admin_level_set", {"admin_user_id": session.get("user_id"), "new_level": level})
    add_notification(
        user_id,
        "admin_level",
        "Уровень изменён администратором",
        f"Ваш игровой уровень установлен на {level}.",
        {"level": level, "admin_user_id": session.get("user_id")},
    )
    return jsonify({"ok": True, "message": "Уровень обновлён"})


@app.post("/api/admin/user/reset_password")
@require_admin
def api_admin_reset_password():
    denied = admin_level_guard(5)
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    user_id = to_int(payload.get("user_id"))
    new_password = str(payload.get("new_password", "")).strip()

    if user_id is None:
        return jsonify({"ok": False, "error": "Некорректный user_id"}), 400
    if len(new_password) < 6:
        return jsonify({"ok": False, "error": "Новый пароль минимум 6 символов"}), 400
    target = get_user_by_id(user_id)
    if not target:
        return jsonify({"ok": False, "error": "Пользователь не найден"}), 404
    denied = guard_root_immunity(target, "сбрасывать пароль")
    if denied:
        return denied

    db = get_db()
    db.execute("UPDATE users SET password_hash = ? WHERE id = ?", (hash_password(new_password), user_id))
    db.commit()
    log_action(user_id, "admin_password_reset", {"admin_user_id": session.get("user_id")})
    add_notification(
        user_id,
        "admin_password_reset",
        "Пароль изменён администратором",
        "Ваш пароль был сброшен администратором. Рекомендуется сразу сменить его в профиле.",
        {"admin_user_id": session.get("user_id")},
    )
    return jsonify({"ok": True, "message": "Пароль пользователя обновлён"})


@app.post("/api/admin/user/delete")
@require_admin
def api_admin_delete_user():
    denied = admin_level_guard(5)
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    user_id = to_int(payload.get("user_id"))
    if user_id is None:
        return jsonify({"ok": False, "error": "Некорректный user_id"}), 400
    if int(user_id) == int(session.get("user_id")):
        return jsonify({"ok": False, "error": "Нельзя удалить самого себя через админ-панель"}), 400

    target = get_user_by_id(user_id)
    if not target:
        return jsonify({"ok": False, "error": "Пользователь не найден"}), 404
    denied = guard_root_immunity(target, "удалять аккаунт")
    if denied:
        return denied
    if target["username"].lower() == "admin" and not bool(getattr(g, "is_root_admin", False)):
        return jsonify({"ok": False, "error": "Пользователя admin удалить нельзя"}), 400

    db = get_db()
    db.execute("UPDATE users SET ref_by = NULL WHERE ref_by = ?", (user_id,))
    db.execute("DELETE FROM cards WHERE user_id = ?", (user_id,))
    db.execute("DELETE FROM credits WHERE user_id = ?", (user_id,))
    db.execute("DELETE FROM support_messages WHERE user_id = ? OR sender_user_id = ?", (user_id, user_id))
    db.execute("DELETE FROM admin_chat_messages WHERE sender_user_id = ?", (user_id,))
    db.execute("DELETE FROM public_chat_messages WHERE sender_user_id = ?", (user_id,))
    purge_user_messenger_data(user_id)
    db.execute("DELETE FROM notifications WHERE user_id = ?", (user_id,))
    db.execute("DELETE FROM logs WHERE user_id = ?", (user_id,))
    db.execute("DELETE FROM users WHERE id = ?", (user_id,))
    db.commit()
    ANTI_ABUSE_STATE.pop(int(user_id), None)

    log_action(
        int(session.get("user_id")),
        "admin_user_deleted",
        {"deleted_user_id": int(user_id), "deleted_username": target["username"]},
    )
    return jsonify({"ok": True, "message": f"Пользователь {target['username']} удалён из базы"})


@app.post("/api/admin/credit/extend")
@require_admin
def api_admin_extend_credit():
    denied = admin_level_guard(5)
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    credit_id = to_int(payload.get("credit_id"))
    days = to_int(payload.get("days"))
    if credit_id is None or days is None or days < 1 or days > 30:
        return jsonify({"ok": False, "error": "Продление возможно только на 1-30 дней"}), 400

    db = get_db()
    credit = db.execute("SELECT * FROM credits WHERE id = ? AND repaid = 0", (credit_id,)).fetchone()
    if not credit:
        return jsonify({"ok": False, "error": "Активный кредит не найден"}), 404

    due_at = parse_iso(credit["due_at"]) or now_utc()
    new_due = due_at + timedelta(days=days)
    db.execute("UPDATE credits SET due_at = ? WHERE id = ?", (to_iso(new_due), credit_id))
    db.commit()
    log_action(int(credit["user_id"]), "admin_credit_extend", {"admin_user_id": session.get("user_id"), "credit_id": credit_id, "days": days})
    add_notification(
        int(credit["user_id"]),
        "admin_credit_extend",
        "Продлён дедлайн кредита",
        f"Администратор продлил дедлайн кредита на {days} дней.",
        {"credit_id": credit_id, "days": days, "admin_user_id": session.get("user_id")},
    )
    return jsonify({"ok": True, "message": "Дедлайн кредита продлён"})


@app.post("/api/admin/credit/close")
@require_admin
def api_admin_close_credit():
    denied = admin_level_guard(5)
    if denied:
        return denied
    payload = request.get_json(silent=True) or {}
    credit_id = to_int(payload.get("credit_id"))
    if credit_id is None:
        return jsonify({"ok": False, "error": "Некорректный credit_id"}), 400

    db = get_db()
    credit = db.execute("SELECT * FROM credits WHERE id = ? AND repaid = 0", (credit_id,)).fetchone()
    if not credit:
        return jsonify({"ok": False, "error": "Активный кредит не найден"}), 404

    db.execute("UPDATE credits SET repaid = 1 WHERE id = ?", (credit_id,))
    db.commit()
    log_action(int(credit["user_id"]), "admin_credit_closed", {"admin_user_id": session.get("user_id"), "credit_id": credit_id})
    add_notification(
        int(credit["user_id"]),
        "admin_credit_closed",
        "Кредит закрыт администратором",
        "Активный кредит был принудительно закрыт администратором.",
        {"credit_id": credit_id, "admin_user_id": session.get("user_id")},
    )
    return jsonify({"ok": True, "message": "Кредит принудительно закрыт"})


@app.get("/api/vip/config")
@require_auth
def api_vip_config():
    prices = {str(level): cost for level, cost in VIP_UPGRADE_COSTS.items()}
    return jsonify({"ok": True, "vip": VIP_CONFIG, "upgrade_costs": prices})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    print(f"NM-Bank started: http://127.0.0.1:{port} (LAN: http://0.0.0.0:{port})")
    app.run(host="0.0.0.0", port=port, debug=False)

