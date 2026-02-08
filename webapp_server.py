from __future__ import annotations

import hashlib
import hmac
import json
import logging
import secrets
import time
from urllib.parse import parse_qsl
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import httpx

from config import TG_TOKEN, BOT_USERNAME, TAROT_PACKS
from db_layer.billing import get_billing_snapshot
from db_layer.support import log_api_error
from db_layer.users import get_user

API_VERSION = "1.0"
SESSION_TTL_SEC = 24 * 60 * 60
SUPPORT_URL = "https://t.me/astraaisupport"

logger = logging.getLogger("webapp_api")


APP_DIR = Path(__file__).resolve().parent
STATIC_DIR = APP_DIR / "miniapp"


class AuthRequest(BaseModel):
    initData: str


class SessionRequest(BaseModel):
    session: str


class InvoiceRequest(BaseModel):
    session: str
    pack_key: str


def _check_init_data(init_data: str, bot_token: str) -> Dict[str, Any]:
    if not init_data or not bot_token:
        raise ValueError("missing initData or bot token")

    data = dict(parse_qsl(init_data, strict_parsing=True))
    hash_value = data.pop("hash", None)
    if not hash_value:
        raise ValueError("hash missing")

    # build data check string
    data_check = "\n".join([f"{k}={v}" for k, v in sorted(data.items())])
    secret = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
    calc_hash = hmac.new(secret, data_check.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(calc_hash, hash_value):
        raise ValueError("invalid hash")

    user_json = data.get("user")
    if not user_json:
        raise ValueError("user missing")
    try:
        user = json.loads(user_json)
    except Exception:
        raise ValueError("user json invalid")
    return user


def _make_session(user_id: int) -> str:
    ts = int(time.time())
    nonce = secrets.token_hex(6)
    raw = f"{user_id}:{ts}:{nonce}"
    sig = hmac.new(TG_TOKEN.encode(), raw.encode(), hashlib.sha256).hexdigest()
    return f"{raw}:{sig}"


def _verify_session(session: str) -> Optional[int]:
    try:
        parts = (session or "").split(":")
        if len(parts) != 4:
            return None
        user_id = int(parts[0])
        ts = int(parts[1])
        nonce = parts[2]
        sig = parts[3]
        raw = f"{user_id}:{ts}:{nonce}"
        calc = hmac.new(TG_TOKEN.encode(), raw.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(calc, sig):
            return None
        if (time.time() - ts) > SESSION_TTL_SEC:
            return None
        return user_id
    except Exception:
        return None


def _get_user_id_from_session(session: str) -> int:
    user_id = _verify_session(session)
    if user_id:
        return int(user_id)
    raise ValueError("session_invalid")


def _error_response(code: str, status: int, message: str) -> JSONResponse:
    return JSONResponse(
        {"ok": False, "error": code, "message": message, "version": API_VERSION},
        status_code=status,
    )


app = FastAPI(title="Astra MiniApp")


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logger.warning("validation_error path=%s", request.url.path)
    return _error_response("bad_request", 422, "invalid request body")


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("server_error path=%s", request.url.path)
    return _error_response("server_error", 500, "internal server error")


@app.post("/api/auth")
def api_auth(payload: AuthRequest):
    try:
        user = _check_init_data(payload.initData, TG_TOKEN)
    except Exception:
        return _error_response("auth_failed", 401, "telegram auth failed")

    user_id = int(user.get("id"))
    chat_id = user_id
    try:
        get_user(user_id)
    except Exception:
        logger.exception("user_bootstrap_failed user_id=%s", user_id)
        try:
            log_api_error(user_id=user_id, endpoint="/api/auth", status_code=500, error_text="user_bootstrap_failed")
        except Exception:
            pass
        return _error_response("server_error", 500, "failed to load user")
    try:
        snap = get_billing_snapshot(user_id, chat_id)
    except Exception:
        logger.exception("billing_snapshot_failed user_id=%s", user_id)
        try:
            log_api_error(user_id=user_id, endpoint="/api/auth", status_code=500, error_text="billing_snapshot_failed")
        except Exception:
            pass
        return _error_response("server_error", 500, "failed to load billing")
    free_left = int(snap.get("tarot_free_left", 0))
    credits = int(snap.get("tarot_credits", 0))
    balance = free_left + credits

    if not BOT_USERNAME:
        ref_link = ""
    else:
        ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{user_id}"

    session = _make_session(user_id)
    return {
        "ok": True,
        "version": API_VERSION,
        "user": {
            "id": user_id,
            "first_name": user.get("first_name", ""),
            "last_name": user.get("last_name", ""),
            "username": user.get("username", ""),
        },
        "balance": balance,
        "balance_breakdown": {
            "free_left": free_left,
            "credits": credits,
            "total": balance,
        },
        "ref_link": ref_link,
        "support_link": SUPPORT_URL,
        "packages": TAROT_PACKS,
        "session": session,
    }


@app.post("/api/balance")
def api_balance(payload: SessionRequest):
    try:
        user_id = _get_user_id_from_session(payload.session)
    except Exception:
        return _error_response("auth_failed", 401, "session invalid or expired")

    chat_id = user_id
    try:
        snap = get_billing_snapshot(user_id, chat_id)
    except Exception:
        logger.exception("billing_snapshot_failed user_id=%s", user_id)
        try:
            log_api_error(user_id=user_id, endpoint="/api/balance", status_code=500, error_text="billing_snapshot_failed")
        except Exception:
            pass
        return _error_response("server_error", 500, "failed to load billing")
    balance = int(snap.get("tarot_free_left", 0)) + int(snap.get("tarot_credits", 0))
    return {"ok": True, "version": API_VERSION, "balance": balance}


@app.post("/api/invoice")
def api_invoice(payload: InvoiceRequest):
    try:
        user_id = _get_user_id_from_session(payload.session)
    except Exception:
        return _error_response("auth_failed", 401, "session invalid or expired")

    pack_key = str(payload.pack_key or "").strip()
    pack = next((p for p in TAROT_PACKS if p["key"] == pack_key), None)
    if not pack:
        return _error_response("bad_pack", 400, "unknown pack")

    spreads = int(pack["spreads"])
    stars = int(pack["stars"])
    payload_id = f"tarot_pack_{pack_key}:user:{user_id}:ts:{int(time.time())}"

    logger.info("INVOICE_REQUEST user_id=%s pack=%s", user_id, pack_key)

    try:
        resp = httpx.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/createInvoiceLink",
            json={
                "title": f"{spreads} раскладов",
                "description": "Покупка пакета раскладов.",
                "payload": payload_id,
                "provider_token": "",
                "currency": "XTR",
                "prices": [{"label": f"{spreads} раскладов", "amount": stars}],
            },
            timeout=10.0,
        )
        data = resp.json()
        if not data.get("ok"):
            logger.warning("INVOICE_LINK_FAILED user_id=%s pack=%s resp=%s", user_id, pack_key, data)
            return _error_response("invoice_failed", 500, "failed to create invoice link")
        link = data.get("result")
        logger.info("INVOICE_LINK_CREATED user_id=%s pack=%s payload=%s", user_id, pack_key, payload_id)
        return {"ok": True, "invoice_link": link}
    except Exception:
        logger.exception("INVOICE_LINK_EXCEPTION user_id=%s pack=%s", user_id, pack_key)
        return _error_response("invoice_failed", 500, "failed to create invoice link")


@app.get("/api/health")
def api_health():
    return {"ok": True, "version": API_VERSION}


# static miniapp
if STATIC_DIR.exists():
    app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="miniapp")
