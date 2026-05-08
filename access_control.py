import hmac
import hashlib
import json
import os
import re
from pathlib import Path

from shared import log


BUILTIN_OWNER_IDS = {6205099620}


def _env_list(*names):
    values = []
    for name in names:
        raw = os.environ.get(name, "")
        if raw:
            values.extend(part.strip() for part in re.split(r"[,;\n]+", raw))
    return [value for value in values if value]


def _env_ids(*names):
    ids = set()
    for value in _env_list(*names):
        try:
            ids.add(int(value))
        except ValueError:
            log.warning("Некорректный Telegram ID в настройках доступа: %s", value)
    return ids


def _access_file_path():
    default_path = "/data/authorized_users.json" if Path("/data").exists() else "authorized_users.json"
    raw = os.environ.get("BOT_ACCESS_FILE", default_path)
    path = Path(raw)
    if path.is_absolute():
        return path
    return Path(__file__).resolve().parent / path


PERSONAL_ACCESS_CODES = _env_list(
    "BOT_PERSONAL_ACCESS_CODES",
    "BOT_INVITE_CODES",
    "BOT_ACCESS_CODES",
    "ACCESS_CODES",
    "PERSONAL_ACCESS_CODES",
)
OWNER_IDS = BUILTIN_OWNER_IDS | _env_ids("BOT_OWNER_IDS", "ADMIN_IDS")
STATIC_USER_IDS = _env_ids("AUTHORIZED_USER_IDS", "BOT_AUTHORIZED_USER_IDS")
ACCESS_FILE = _access_file_path()

_access_cache = None


def access_enabled():
    return bool(PERSONAL_ACCESS_CODES or OWNER_IDS or STATIC_USER_IDS)


def _empty_access_data():
    return {"authorized_user_ids": set(), "personal_code_users": {}}


def _code_hash(code):
    return hashlib.sha256(str(code).encode("utf-8")).hexdigest()


def _constant_time_text_equal(left, right):
    left_bytes = str(left or "").encode("utf-8")
    right_bytes = str(right or "").encode("utf-8")
    return hmac.compare_digest(left_bytes, right_bytes)


def _load_access_data():
    global _access_cache
    if _access_cache is not None:
        return {
            "authorized_user_ids": set(_access_cache["authorized_user_ids"]),
            "personal_code_users": dict(_access_cache["personal_code_users"]),
        }
    if not ACCESS_FILE.exists():
        _access_cache = _empty_access_data()
        return _load_access_data()
    try:
        data = json.loads(ACCESS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        log.warning("Не удалось прочитать файл доступа %s: %s", ACCESS_FILE, exc)
        _access_cache = _empty_access_data()
        return _load_access_data()

    if isinstance(data, dict):
        raw_ids = data.get("authorized_user_ids", [])
        raw_code_users = data.get("personal_code_users", {})
    elif isinstance(data, list):
        raw_ids = data
        raw_code_users = {}
    else:
        raw_ids = []
        raw_code_users = {}

    ids = set()
    for value in raw_ids:
        try:
            ids.add(int(value))
        except (TypeError, ValueError):
            continue

    code_users = {}
    if isinstance(raw_code_users, dict):
        for code_key, value in raw_code_users.items():
            try:
                code_users[str(code_key)] = int(value)
            except (TypeError, ValueError):
                continue

    _access_cache = {"authorized_user_ids": ids, "personal_code_users": code_users}
    return _load_access_data()


def _save_access_data(data):
    global _access_cache
    ids = set(data.get("authorized_user_ids", set()))
    code_users = dict(data.get("personal_code_users", {}))
    ACCESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    ACCESS_FILE.write_text(
        json.dumps(
            {
                "authorized_user_ids": sorted(ids),
                "personal_code_users": code_users,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    _access_cache = {"authorized_user_ids": ids, "personal_code_users": code_users}


def _load_authorized_ids():
    return set(_load_access_data()["authorized_user_ids"])


def _matching_personal_code_hash(code):
    for valid_code in PERSONAL_ACCESS_CODES:
        if _constant_time_text_equal(code, valid_code):
            return _code_hash(valid_code)
    return None


def is_authorized(user_id):
    if not access_enabled():
        return True
    if user_id is None:
        return False
    try:
        user_id = int(user_id)
    except (TypeError, ValueError):
        return False
    if user_id in OWNER_IDS or user_id in STATIC_USER_IDS:
        return True
    return user_id in _load_authorized_ids()


def validate_access_code(text):
    code = str(text or "").strip()
    if not code:
        return False
    if _matching_personal_code_hash(code):
        return True
    return False


def authorize_user(user_id):
    if user_id is None:
        return False
    try:
        user_id = int(user_id)
    except (TypeError, ValueError):
        return False
    data = _load_access_data()
    data["authorized_user_ids"].add(user_id)
    try:
        _save_access_data(data)
    except OSError as exc:
        log.warning("Не удалось сохранить доступ для %s: %s", user_id, exc)
        return False
    return True


def authorize_by_code(user_id, text):
    code = str(text or "").strip()
    if not code:
        return False, "bad"
    try:
        user_id = int(user_id)
    except (TypeError, ValueError):
        return False, "bad"

    personal_code_hash = _matching_personal_code_hash(code)
    if not personal_code_hash:
        return False, "bad"

    data = _load_access_data()
    used_by = data["personal_code_users"].get(personal_code_hash)
    if used_by is not None and used_by != user_id:
        return False, "used"

    data["authorized_user_ids"].add(user_id)
    data["personal_code_users"][personal_code_hash] = user_id
    try:
        _save_access_data(data)
    except OSError as exc:
        log.warning("Не удалось сохранить личный код для %s: %s", user_id, exc)
        return False, "save"
    return True, "ok"


def access_prompt_text():
    if PERSONAL_ACCESS_CODES:
        return (
            "🔐 <b>Доступ закрыт</b>\n\n"
            "Введи свой код доступа одним сообщением. После правильного кода меню откроется автоматически."
        )
    return (
        "🔐 <b>Доступ закрыт</b>\n\n"
        "Код доступа на сервере не настроен. Попроси владельца добавить твой Telegram ID."
    )
