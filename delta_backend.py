#!/usr/bin/env python3
"""
Local Delta Exchange India backend (no external dependencies).
Now also serves index.html at / for Hugging Face Spaces deployment.
Port: 7860 (HF Spaces default)
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import math
import os
import secrets
import socket
import time
import xml.etree.ElementTree as ET
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote_plus, urlencode, urlparse
from urllib.request import Request, urlopen


DELTA_BASE_LIVE = "https://api.india.delta.exchange"
DELTA_BASE_DEMO = "https://cdn-ind.testnet.deltaex.org"
USER_AGENT = "delta-local-backend/1.0"
LOGS: deque[dict[str, Any]] = deque(maxlen=500)
TRADE_HISTORY: deque[dict[str, Any]] = deque(maxlen=500)
MARKET_CACHE: dict[str, dict[str, Any]] = {}
CANDLE_CACHE: dict[str, dict[str, Any]] = {}
PROFILE_CACHE: dict[str, dict[str, Any]] = {}
TRADE_COOLDOWNS: dict[str, float] = {}
SESSIONS: dict[str, dict[str, Any]] = {}
NEWS_CACHE: dict[str, dict[str, Any]] = {}
DELTA_APP_SYMBOL_TO_MARKET: dict[str, str] = {
    "BTCUSDT": "BTCUSD",
    "ETHUSDT": "ETHUSD",
    "SOLUSDT": "SOLUSD",
    "PAXGUSDT": "PAXGUSD",
}
DELTA_MARKET_TO_APP_SYMBOL: dict[str, str] = {v: k for k, v in DELTA_APP_SYMBOL_TO_MARKET.items()}
DELTA_RESOLUTION_SECONDS: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
}
KNOWN_BAD_PRODUCT_IDS_BY_MODE: dict[str, dict[str, str]] = {
    "live": {
        "58205": "14823",
    },
    "demo": {
        "2697": "84",
        "2698": "1699",
    },
}

# Path to app HTML (same directory as this script)
APP_DIR = os.path.dirname(os.path.abspath(__file__))
INDEX_HTML_PATH = os.path.join(APP_DIR, "index_price_news.html")
if not os.path.exists(INDEX_HTML_PATH):
    INDEX_HTML_PATH = os.path.join(APP_DIR, "index.html")
PRICE_CACHE_TTL_SEC = 0.35
CANDLE_CACHE_TTL_SEC = 0.75
PROFILE_CACHE_TTL_SEC = 5.0
TRADE_COOLDOWN_SEC = 20.0
SESSION_TTL_SEC = 12 * 60 * 60
APP_LOGIN_ID = str(os.getenv("APP_USER_ID") or "").strip()
APP_LOGIN_PASS = str(os.getenv("APP_USER_PASS") or "").strip()
AUTH_REQUIRED = bool(APP_LOGIN_ID and APP_LOGIN_PASS)
BACKEND_STORE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backend_store.json")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def utc_day_key() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def add_log(event: str, **meta: Any) -> None:
    row = {"ts": utc_now(), "event": event, **meta}
    LOGS.append(row)
    print(json.dumps(row, ensure_ascii=False))


def load_backend_store() -> dict[str, Any]:
    try:
        with open(BACKEND_STORE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def save_backend_store(data: dict[str, Any]) -> None:
    try:
        with open(BACKEND_STORE_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        add_log("backend_store_save_failed", error=str(e))


BACKEND_STORE: dict[str, Any] = load_backend_store()
BACKEND_STORE.setdefault("creds", {"live": {"key": "", "secret": ""}, "demo": {"key": "", "secret": ""}})


def get_saved_creds(mode: str) -> tuple[str, str]:
    m = normalize_mode(mode)
    creds = (BACKEND_STORE.get("creds") or {}).get(m) or {}
    key = str(creds.get("key") or "").strip()
    secret = str(creds.get("secret") or "").strip()
    return key, secret


def save_creds(mode: str, key: str, secret: str) -> None:
    m = normalize_mode(mode)
    creds = BACKEND_STORE.setdefault("creds", {"live": {"key": "", "secret": ""}, "demo": {"key": "", "secret": ""}})
    creds[m] = {"key": str(key or "").strip(), "secret": str(secret or "").strip()}
    save_backend_store(BACKEND_STORE)


def normalize_mode(mode: str | None) -> str:
    return "demo" if (mode or "").strip().lower() == "demo" else "live"


def delta_base(mode: str | None) -> str:
    return DELTA_BASE_DEMO if normalize_mode(mode) == "demo" else DELTA_BASE_LIVE


def compact_json(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


def short_text(txt: Any, n: int = 500) -> str:
    s = str(txt or "")
    return s if len(s) <= n else (s[:n] + "...")


def best_error(parsed: Any, raw: str) -> str:
    if isinstance(parsed, dict):
        for k in ("error", "message", "msg", "detail"):
            v = parsed.get(k)
            if v:
                return short_text(v, 600)
    return short_text(raw, 600)


def request_json(
    method: str,
    url: str,
    headers: dict[str, str] | None = None,
    payload_text: str | None = None,
    timeout: int = 20,
) -> tuple[int, dict[str, str], Any, str]:
    data = payload_text.encode("utf-8") if payload_text is not None else None
    req = Request(url=url, data=data, method=method.upper())
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            status = int(getattr(resp, "status", 200))
            resp_headers = dict(resp.headers.items())
    except HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        status = int(e.code)
        resp_headers = dict(e.headers.items()) if e.headers else {}
    except URLError as e:
        raise RuntimeError(f"Network error: {e.reason}") from e
    try:
        parsed: Any = json.loads(raw) if raw else {}
    except Exception:
        parsed = {"raw": raw}
    return status, resp_headers, parsed, raw


def query_string(query: dict[str, Any] | None) -> str:
    if not query:
        return ""
    clean: dict[str, Any] = {}
    for k, v in query.items():
        if v is None:
            continue
        clean[k] = v
    if not clean:
        return ""
    return "?" + urlencode(clean, doseq=True)


def signed_delta_call(
    key: str,
    secret: str,
    mode: str,
    method: str,
    path: str,
    query: dict[str, Any] | None = None,
    body: dict[str, Any] | None = None,
) -> tuple[int, Any, str]:
    m = method.upper()
    qs = query_string(query)
    payload = compact_json(body) if body is not None else ""
    ts = str(int(time.time()))
    sign_input = m + ts + path + qs + payload
    signature = hmac.new(
        secret.encode("utf-8"),
        sign_input.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    headers = {
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
        "api-key": key,
        "timestamp": ts,
        "signature": signature,
    }
    if payload:
        headers["Content-Type"] = "application/json"

    url = delta_base(mode) + path + qs
    status, _, parsed, raw = request_json(m, url, headers=headers, payload_text=(payload or None))
    return status, parsed, raw


def public_delta_call(
    mode: str,
    path: str,
    query: dict[str, Any] | None = None,
) -> tuple[int, Any, str]:
    url = delta_base(mode) + path + query_string(query)
    headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
    status, _, parsed, raw = request_json("GET", url, headers=headers, payload_text=None)
    return status, parsed, raw


def as_int_if_possible(v: Any) -> Any:
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return v
    if isinstance(v, str) and v.strip().isdigit():
        return int(v.strip())
    return v


def safe_int(v: Any, default: int) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def normalize_market_symbol(sym: str) -> str:
    s = str(sym or "").strip().upper()
    if not s:
        return s
    if s in DELTA_APP_SYMBOL_TO_MARKET:
        return DELTA_APP_SYMBOL_TO_MARKET[s]
    if s.endswith("USDT") and len(s) > 4:
        return s[:-4] + "USD"
    return s


def remap_product_id(mode: str, pid: Any) -> Any:
    s = str(pid or "").strip()
    if not s:
        return pid
    fixed = KNOWN_BAD_PRODUCT_IDS_BY_MODE.get(normalize_mode(mode), {}).get(s, s)
    if fixed.isdigit():
        return int(fixed)
    return fixed


def payload_result(payload: Any) -> Any:
    if isinstance(payload, dict) and "result" in payload:
        return payload.get("result")
    return payload


def cache_get(cache: dict[str, dict[str, Any]], key: str, ttl_sec: float) -> Any:
    row = cache.get(key)
    if not row:
        return None
    if (time.time() - float(row.get("ts", 0.0))) > ttl_sec:
        cache.pop(key, None)
        return None
    return row.get("value")


def cache_put(cache: dict[str, dict[str, Any]], key: str, value: Any) -> Any:
    cache[key] = {"ts": time.time(), "value": value}
    return value


def actor_key(mode: str, key: str) -> str:
    digest = hashlib.sha256(f"{normalize_mode(mode)}:{key}".encode("utf-8")).hexdigest()
    return digest[:24]


def cooldown_cache_key(mode: str, key: str, product_id: Any) -> str:
    return f"{actor_key(mode, key)}:{remap_product_id(mode, product_id)}"


def cooldown_remaining(mode: str, key: str, product_id: Any) -> int:
    until = float(TRADE_COOLDOWNS.get(cooldown_cache_key(mode, key, product_id), 0.0) or 0.0)
    remaining = int(math.ceil(until - time.time()))
    return max(0, remaining)


def start_cooldown(mode: str, key: str, product_id: Any) -> int:
    until = time.time() + TRADE_COOLDOWN_SEC
    TRADE_COOLDOWNS[cooldown_cache_key(mode, key, product_id)] = until
    return int(until)


def extract_available_balance(profile: Any) -> float | None:
    result = payload_result(profile)
    if not isinstance(result, dict):
        return None
    direct_fields = [
        "available_balance",
        "availableBalance",
        "available_margin",
        "availableMargin",
        "free_balance",
        "freeBalance",
        "balance",
        "wallet_balance",
        "walletBalance",
        "margin_balance",
        "marginBalance",
        "cash_balance",
        "cashBalance",
        "equity",
    ]
    for field in direct_fields:
        raw = result.get(field)
        val = to_float(raw, float("nan"))
        if math.isfinite(val):
            return val
    wallet_sources = [
        result.get("wallet"),
        result.get("wallets"),
        result.get("balances"),
        result.get("asset_balances"),
    ]
    preferred_codes = {"USD", "USDT", "INR"}
    wallet_fields = (
        "available_balance",
        "available",
        "free",
        "free_balance",
        "balance",
        "wallet_balance",
        "margin_balance",
        "cash_balance",
        "equity",
    )
    for wallets in wallet_sources:
        if isinstance(wallets, dict):
            iterable = []
            for code, row in wallets.items():
                if isinstance(row, dict):
                    iterable.append((str(code).upper(), row))
                else:
                    iterable.append((str(code).upper(), {"balance": row}))
        elif isinstance(wallets, list):
            iterable = []
            for row in wallets:
                if not isinstance(row, dict):
                    continue
                code = str(row.get("asset_symbol") or row.get("asset") or row.get("currency") or row.get("code") or "").upper()
                iterable.append((code, row))
        else:
            iterable = []
        for preferred_only in (True, False):
            for code, row in iterable:
                if preferred_only and code and code not in preferred_codes:
                    continue
                for field in wallet_fields:
                    val = to_float(row.get(field), float("nan"))
                    if math.isfinite(val):
                        return val
    return None


def summarize_profile(mode: str, payload: Any) -> dict[str, Any]:
    result = payload_result(payload)
    if not isinstance(result, dict):
        return {"mode": normalize_mode(mode), "available_balance": None}
    available = extract_available_balance(result)
    return {
        "mode": normalize_mode(mode),
        "user_id": result.get("id") or result.get("user_id") or result.get("uid"),
        "email": result.get("email"),
        "name": result.get("name") or result.get("full_name"),
        "available_balance": available,
        "raw_keys": sorted(result.keys())[:20],
    }


def record_trade_event(event: str, mode: str, product_id: Any, **meta: Any) -> None:
    row = {
        "ts": utc_now(),
        "event": event,
        "mode": normalize_mode(mode),
        "product_id": remap_product_id(mode, product_id),
        **meta,
    }
    TRADE_HISTORY.appendleft(row)


def issue_session(user_id: str) -> str:
    token = secrets.token_urlsafe(24)
    SESSIONS[token] = {"user_id": user_id, "expires_at": time.time() + SESSION_TTL_SEC}
    return token


def validate_session(token: str | None) -> dict[str, Any] | None:
    tok = str(token or "").strip()
    if not tok:
        return None
    row = SESSIONS.get(tok)
    if not row:
        return None
    if time.time() >= float(row.get("expires_at", 0.0)):
        SESSIONS.pop(tok, None)
        return None
    return row


def detect_public_ip() -> tuple[str | None, str | None]:
    ipv4 = None
    ipv6_or_any = None
    sources = [
        "https://api.ipify.org?format=json",
        "https://api64.ipify.org?format=json",
    ]
    for i, src in enumerate(sources):
        try:
            status, _, parsed, raw = request_json("GET", src, headers={"User-Agent": USER_AGENT}, timeout=8)
            if status >= 400:
                continue
            ip = ""
            if isinstance(parsed, dict):
                ip = str(parsed.get("ip") or "").strip()
            if not ip and raw:
                ip = raw.strip()
            if not ip:
                continue
            if i == 0:
                ipv4 = ip
            else:
                ipv6_or_any = ip
        except Exception:
            continue
    return ipv4, ipv6_or_any


def local_ips() -> list[str]:
    out: set[str] = set()
    try:
        _, _, ips = socket.gethostbyname_ex(socket.gethostname())
        for ip in ips:
            if ip:
                out.add(ip)
    except Exception:
        pass
    return sorted(out)


NEWS_SYMBOL_HINTS: dict[str, str] = {
    "BTCUSDT": "bitcoin",
    "ETHUSDT": "ethereum",
    "SOLUSDT": "solana",
    "PAXGUSDT": "gold",
}


def sentiment_from_title(title: str) -> int:
    txt = (title or "").lower()
    pos = ("surge", "jump", "breakout", "bull", "approval", "rally", "strong", "record", "gain")
    neg = ("crash", "drop", "bear", "lawsuit", "hack", "ban", "liquidation", "fear", "weak")
    score = 0
    for w in pos:
        if w in txt:
            score += 2
    for w in neg:
        if w in txt:
            score -= 2
    return score


def fetch_news_sentiment(symbol: str) -> dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    key = NEWS_SYMBOL_HINTS.get(sym, sym.replace("USDT", "").lower() or "crypto")
    cache_key = f"news:{key}"
    cached = cache_get(NEWS_CACHE, cache_key, 300.0)
    if cached is not None:
        return cached

    query = quote_plus(f"{key} crypto when:1d")
    url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
    titles: list[str] = []
    try:
        status, _, _, raw = request_json("GET", url, headers={"User-Agent": USER_AGENT}, payload_text=None, timeout=12)
        if status >= 400:
            raise RuntimeError(short_text(raw, 160))
        root = ET.fromstring(raw)
        for node in root.findall(".//item/title"):
            t = str(node.text or "").strip()
            if t:
                titles.append(t)
            if len(titles) >= 15:
                break
    except Exception:
        out = {
            "ok": False,
            "symbol": sym,
            "bias": "neutral",
            "score": 0,
            "top_headline": "",
            "count": 0,
            "ts": utc_now(),
        }
        return cache_put(NEWS_CACHE, cache_key, out)

    score = sum(sentiment_from_title(t) for t in titles)
    if score >= 6:
        bias = "bullish"
    elif score <= -6:
        bias = "bearish"
    else:
        bias = "neutral"
    out = {
        "ok": True,
        "symbol": sym,
        "bias": bias,
        "score": score,
        "top_headline": titles[0] if titles else "",
        "count": len(titles),
        "ts": utc_now(),
    }
    return cache_put(NEWS_CACHE, cache_key, out)


def product_candidates(products: list[dict[str, Any]], target: str) -> list[tuple[int, Any]]:
    base = target.replace("USDT", "")
    picks: list[tuple[int, Any]] = []
    for p in products:
        pid = p.get("id")
        if pid is None:
            continue
        sym = str(p.get("symbol") or p.get("product_symbol") or "").upper()
        ctype = str(p.get("contract_type") or "").lower()
        state = str(p.get("state") or "").lower()
        ud = p.get("underlying_asset") if isinstance(p.get("underlying_asset"), dict) else {}
        qd = p.get("quoting_asset") if isinstance(p.get("quoting_asset"), dict) else {}
        underlying = str(
            ud.get("symbol")
            or p.get("underlying_asset_symbol")
            or p.get("underlying_symbol")
            or ""
        ).upper()
        quote = str(
            qd.get("symbol")
            or p.get("quoting_asset_symbol")
            or p.get("quote_asset_symbol")
            or ""
        ).upper()
        desc = str(p.get("description") or "").upper()

        score = 0
        if state == "live":
            score += 30
        if "perpetual" in ctype:
            score += 420
        if "spot" in ctype:
            score -= 160
        if "option" in ctype:
            score -= 260
        if sym == (base + "USD"):
            score += 260
        if sym == target:
            score += 40
        if sym == (base + "_USDT"):
            score += 15
        if underlying == base and quote == "USD":
            score += 240
        if underlying == base and quote == "USDT":
            score += 35
        if sym.startswith(base) and "USD" in sym:
            score += 30
        if sym.startswith(base) and "USDT" in sym:
            score += 10
        if base == "PAXG" and ("GOLD" in desc or "PAXG" in sym):
            score += 30
        if score > 0:
            picks.append((score, pid))
    picks.sort(key=lambda x: x[0], reverse=True)
    return picks


def autofill_map(products: list[dict[str, Any]]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for target in ["BTCUSDT", "ETHUSDT", "SOLUSDT", "PAXGUSDT"]:
        picks = product_candidates(products, target)
        if picks:
            mapping[target] = str(picks[0][1])
    return mapping


def extract_products_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        result = payload.get("result")
        if isinstance(result, list):
            return [x for x in result if isinstance(x, dict)]
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    return []


def extract_tickers_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        result = payload.get("result")
        if isinstance(result, list):
            return [x for x in result if isinstance(x, dict)]
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    return []


def extract_candles_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        result = payload.get("result")
        if isinstance(result, list):
            return [x for x in result if isinstance(x, dict)]
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    return []


def normalize_candles(candles: list[dict[str, Any]], resolution_sec: int | None = None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for c in candles:
        t = safe_int(c.get("time"), 0)
        o = to_float(c.get("open"), 0.0)
        h = to_float(c.get("high"), 0.0)
        l = to_float(c.get("low"), 0.0)
        cl = to_float(c.get("close"), 0.0)
        v = to_float(c.get("volume"), 0.0)
        if t <= 0:
            continue
        if not all(math.isfinite(x) for x in (o, h, l, cl, v)):
            continue
        if resolution_sec and resolution_sec > 0:
            t = (t // resolution_sec) * resolution_sec
        hi = max(h, o, cl, l)
        lo = min(l, o, cl, h)
        if hi < lo:
            hi, lo = lo, hi
        out.append(
            {
                "time": t,
                "open": o,
                "high": hi,
                "low": lo,
                "close": cl,
                "volume": max(0.0, v),
            }
        )
    out.sort(key=lambda x: x["time"])
    dedup: list[dict[str, Any]] = []
    prev_t = None
    for c in out:
        if c["time"] == prev_t:
            dedup[-1] = c
        else:
            dedup.append(c)
            prev_t = c["time"]
    return dedup


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "DeltaLocalBackend/1.0"

    def log_message(self, *_: Any) -> None:
        return

    def _json(self, code: int, data: dict[str, Any]) -> None:
        raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()
        self.wfile.write(raw)

    def _serve_html(self, path: str) -> None:
        """Serve a static HTML file."""
        try:
            with open(path, "rb") as f:
                raw = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(raw)
        except FileNotFoundError:
            err = b"index.html not found"
            self.send_response(404)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(err)))
            self.end_headers()
            self.wfile.write(err)

    def _read_body_json(self) -> dict[str, Any]:
        try:
            ln = int(self.headers.get("Content-Length", "0"))
        except Exception:
            ln = 0
        raw = self.rfile.read(ln) if ln > 0 else b""
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return {}

    def _bearer_token(self) -> str:
        auth = str(self.headers.get("Authorization") or "").strip()
        if auth.lower().startswith("bearer "):
            return auth[7:].strip()
        return ""

    def _require_auth(self) -> dict[str, Any] | None:
        if not AUTH_REQUIRED:
            return {"user_id": "local", "expires_at": time.time() + SESSION_TTL_SEC}
        session = validate_session(self._bearer_token())
        if session:
            return session
        self._json(401, {"ok": False, "error": "Unauthorized", "auth_required": True})
        return None

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        q = parse_qs(parsed.query)

        # â”€â”€ Serve index.html at root â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        if path in ("/", "/index.html", ""):
            self._serve_html(INDEX_HTML_PATH)
            return

        if path == "/health":
            self._json(200, {"ok": True, "ts": utc_now(), "auth_required": AUTH_REQUIRED})
            return

        if path == "/auth/config":
            self._json(200, {"ok": True, "auth_required": AUTH_REQUIRED})
            return

        if path == "/auth/me":
            session = self._require_auth()
            if session is None:
                return
            self._json(200, {
                "ok": True,
                "auth_required": AUTH_REQUIRED,
                "user_id": session.get("user_id"),
                "expires_at": int(session.get("expires_at", 0)),
            })
            return

        if path == "/logs":
            self._json(200, {"ok": True, "count": len(LOGS), "logs": list(LOGS)})
            return

        if path == "/delta/history":
            self._json(200, {"ok": True, "count": len(TRADE_HISTORY), "history": list(TRADE_HISTORY)})
            return

        if path == "/news/sentiment":
            symbol = str((q.get("symbol") or ["BTCUSDT"])[0]).strip().upper() or "BTCUSDT"
            out = fetch_news_sentiment(symbol)
            self._json(200, out)
            return

        if path == "/delta/creds":
            session = self._require_auth()
            if session is None:
                return
            mode = normalize_mode((q.get("mode") or ["live"])[0])
            key, secret = get_saved_creds(mode)
            self._json(200, {
                "ok": True,
                "mode": mode,
                "has_key": bool(key),
                "has_secret": bool(secret),
                "key": key,
                "secret": secret,
            })
            return

        if path == "/ip":
            client_ip = (self.client_address[0] if self.client_address else None) or ""
            xff = self.headers.get("X-Forwarded-For")
            if xff:
                client_ip = xff.split(",")[0].strip() or client_ip
            public_ipv4, public_any = detect_public_ip()
            out = {
                "ok": True,
                "client_ip": client_ip,
                "public_ipv4": public_ipv4,
                "public_ip": public_any or public_ipv4,
                "local_ips": local_ips(),
                "mode_hint": "Whitelist public_ipv4 in Delta API key if required",
            }
            add_log("ip_lookup", client_ip=client_ip, public_ipv4=public_ipv4, public_ip=public_any)
            self._json(200, out)
            return

        if path == "/delta/products":
            mode = normalize_mode((q.get("mode") or ["live"])[0])
            status, payload, raw = public_delta_call(mode, "/v2/products")
            products = extract_products_payload(payload)
            out = {
                "ok": status < 400,
                "mode": mode,
                "base_url": delta_base(mode),
                "count": len(products),
                "result": products if products else payload,
            }
            add_log("delta_products", mode=mode, status=status, count=len(products))
            self._json(200 if status < 400 else status, out if status < 400 else {"ok": False, "error": raw})
            return

        if path == "/delta/autofill":
            mode = normalize_mode((q.get("mode") or ["live"])[0])
            status, payload, raw = public_delta_call(mode, "/v2/products")
            if status >= 400:
                add_log("delta_autofill_failed", mode=mode, status=status)
                self._json(status, {"ok": False, "error": raw})
                return
            products = extract_products_payload(payload)
            mapping = autofill_map(products)
            add_log("delta_autofill", mode=mode, mapped=list(mapping.keys()))
            self._json(200, {"ok": True, "mode": mode, "base_url": delta_base(mode), "mapping": mapping})
            return

        if path == "/delta/ticker":
            mode = normalize_mode((q.get("mode") or ["live"])[0])
            product_id_raw = (q.get("product_id") or [""])[0]
            product_id = remap_product_id(mode, as_int_if_possible(product_id_raw))
            status, payload, raw = public_delta_call(mode, "/v2/tickers", {"contract_types": "perpetual_futures"})
            if status >= 400:
                add_log("delta_ticker_failed", mode=mode, status=status, product_id=product_id)
                self._json(status, {"ok": False, "error": raw})
                return
            tickers = extract_tickers_payload(payload)
            pick = None
            for t in tickers:
                if str(t.get("product_id")) == str(product_id):
                    pick = t
                    break
            if pick is None:
                add_log("delta_ticker_missing", mode=mode, product_id=product_id, count=len(tickers))
                self._json(404, {"ok": False, "error": "Ticker not found", "product_id": product_id})
                return
            add_log("delta_ticker", mode=mode, product_id=product_id)
            self._json(200, {"ok": True, "mode": mode, "product_id": product_id, "ticker": pick})
            return

        if path == "/delta/market":
            mode = normalize_mode((q.get("mode") or ["live"])[0])
            cache_key = f"market:{mode}"
            cached = cache_get(MARKET_CACHE, cache_key, PRICE_CACHE_TTL_SEC)
            if cached is not None:
                self._json(200, cached)
                return
            status, payload, raw = public_delta_call(mode, "/v2/tickers", {"contract_types": "perpetual_futures"})
            if status >= 400:
                add_log("delta_market_failed", mode=mode, status=status)
                self._json(status, {"ok": False, "error": raw})
                return
            tickers = extract_tickers_payload(payload)
            prices: dict[str, Any] = {}
            for t in tickers:
                market_symbol = str(t.get("symbol") or "").upper()
                app_symbol = DELTA_MARKET_TO_APP_SYMBOL.get(market_symbol)
                if not app_symbol:
                    continue
                last = to_float(t.get("mark_price"), 0.0)
                if last <= 0:
                    last = to_float(t.get("spot_price"), 0.0)
                if last <= 0:
                    last = to_float(t.get("close"), 0.0)
                if last <= 0:
                    continue
                change = to_float(t.get("mark_change_24h"), 0.0)
                if abs(change) < 1e-12:
                    change = to_float(t.get("ltp_change_24h"), 0.0)
                if abs(change) < 1e-12:
                    op = to_float(t.get("open"), 0.0)
                    if op > 0:
                        change = ((last - op) / op) * 100.0
                prices[app_symbol] = {
                    "price": last,
                    "change_pct": change,
                    "market_symbol": market_symbol,
                    "product_id": t.get("product_id"),
                }
            out = {"ok": True, "mode": mode, "prices": prices, "cached": False, "ts": utc_now()}
            cache_put(MARKET_CACHE, cache_key, out)
            self._json(200, out)
            return

        if path == "/delta/candles":
            mode = normalize_mode((q.get("mode") or ["live"])[0])
            symbol_in = (q.get("symbol") or [""])[0]
            symbol = normalize_market_symbol(symbol_in)
            resolution = str((q.get("resolution") or ["1m"])[0]).strip()
            if resolution not in DELTA_RESOLUTION_SECONDS:
                self._json(400, {"ok": False, "error": "Unsupported resolution"})
                return

            now_s = int(time.time())
            sec = DELTA_RESOLUTION_SECONDS[resolution]
            limit = max(1, min(5000, safe_int((q.get("limit") or ["300"])[0], 300)))
            end_s = safe_int((q.get("end") or [now_s])[0], now_s)
            if end_s <= 0:
                end_s = now_s
            start_s = safe_int((q.get("start") or [0])[0], 0)
            if start_s <= 0 or start_s >= end_s:
                start_s = max(1, end_s - (sec * max(limit, 200) * 2))

            cache_key = f"candles:{mode}:{symbol}:{resolution}:{start_s}:{end_s}:{limit}"
            cached = cache_get(CANDLE_CACHE, cache_key, CANDLE_CACHE_TTL_SEC)
            if cached is not None:
                self._json(200, cached)
                return

            status, payload, raw = public_delta_call(
                mode,
                "/v2/history/candles",
                {
                    "symbol": symbol,
                    "resolution": resolution,
                    "start": start_s,
                    "end": end_s,
                },
            )
            if status >= 400:
                add_log("delta_candles_failed", mode=mode, status=status, symbol=symbol, resolution=resolution)
                self._json(status, {"ok": False, "error": raw})
                return

            rows = normalize_candles(extract_candles_payload(payload), sec)
            if len(rows) > limit:
                rows = rows[-limit:]

            out = {
                "ok": True,
                "mode": mode,
                "symbol": symbol,
                "resolution": resolution,
                "start": start_s,
                "end": end_s,
                "count": len(rows),
                "candles": rows,
                "cached": False,
                "ts": utc_now(),
            }
            cache_put(CANDLE_CACHE, cache_key, out)
            self._json(200, out)
            return

        self._json(404, {"ok": False, "error": "Not found"})

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        body = self._read_body_json()

        if path == "/webhook/trade":
            add_log("webhook_trade", payload=body)
            self._json(200, {"ok": True})
            return

        if path == "/auth/login":
            if not AUTH_REQUIRED:
                self._json(200, {"ok": True, "auth_required": False, "token": "", "user_id": "local"})
                return
            user_id = str(body.get("user_id") or "").strip()
            password = str(body.get("password") or "")
            if not user_id or not password:
                self._json(400, {"ok": False, "error": "Missing user_id/password", "auth_required": True})
                return
            if user_id != APP_LOGIN_ID or password != APP_LOGIN_PASS:
                self._json(401, {"ok": False, "error": "Invalid credentials", "auth_required": True})
                return
            token = issue_session(user_id)
            self._json(200, {
                "ok": True,
                "auth_required": True,
                "token": token,
                "user_id": user_id,
                "expires_at": int(time.time() + SESSION_TTL_SEC),
            })
            return

        if path == "/auth/logout":
            token = self._bearer_token()
            if token:
                SESSIONS.pop(token, None)
            self._json(200, {"ok": True})
            return

        if path == "/delta/creds/save":
            session = self._require_auth()
            if session is None:
                return
            mode = normalize_mode(str(body.get("mode") or "live"))
            key = str(body.get("key") or "").strip()
            secret = str(body.get("secret") or "").strip()
            if not key or not secret:
                self._json(400, {"ok": False, "error": "Missing key/secret"})
                return
            save_creds(mode, key, secret)
            self._json(200, {"ok": True, "mode": mode, "saved": True})
            return

        if path in ("/delta/test", "/delta/account", "/delta/positions", "/delta/order", "/delta/close", "/delta/orders_all"):
            session = self._require_auth()
            if session is None:
                return
            key = str(body.get("key") or "").strip()
            secret = str(body.get("secret") or "").strip()
            mode = normalize_mode(str(body.get("mode") or "live"))
            if not key or not secret:
                saved_key, saved_secret = get_saved_creds(mode)
                key = key or saved_key
                secret = secret or saved_secret
            if not key or not secret:
                self._json(400, {"ok": False, "error": "Missing key/secret"})
                return
            if str(body.get("key") or "").strip() and str(body.get("secret") or "").strip():
                save_creds(mode, key, secret)

            if path == "/delta/test":
                status, payload, raw = signed_delta_call(
                    key=key, secret=secret, mode=mode, method="GET", path="/v2/profile",
                )
                add_log("delta_test", mode=mode, status=status)
                if status >= 400:
                    self._json(status, {"ok": False, "error": best_error(payload, raw), "result": payload})
                    return
                summary = summarize_profile(mode, payload)
                cache_put(PROFILE_CACHE, actor_key(mode, key), summary)
                self._json(200, {"ok": True, "mode": mode, "result": payload, "account": summary})
                return

            if path == "/delta/account":
                cache_key = actor_key(mode, key)
                cached = cache_get(PROFILE_CACHE, cache_key, PROFILE_CACHE_TTL_SEC)
                if cached is not None:
                    self._json(200, {"ok": True, "mode": mode, "account": cached, "cached": True})
                    return
                status, payload, raw = signed_delta_call(
                    key=key, secret=secret, mode=mode, method="GET", path="/v2/profile",
                )
                add_log("delta_account", mode=mode, status=status)
                if status >= 400:
                    self._json(status, {"ok": False, "error": best_error(payload, raw), "result": payload})
                    return
                summary = summarize_profile(mode, payload)
                cache_put(PROFILE_CACHE, cache_key, summary)
                self._json(200, {"ok": True, "mode": mode, "account": summary, "cached": False})
                return

            if path == "/delta/positions":
                symbol = normalize_market_symbol(str(body.get("symbol") or ""))
                status, payload, raw = signed_delta_call(
                    key=key, secret=secret, mode=mode, method="GET", path="/v2/positions",
                )
                add_log("delta_positions", mode=mode, status=status, symbol=symbol or "")
                if status >= 400:
                    self._json(status, {"ok": False, "error": best_error(payload, raw), "result": payload})
                    return
                rows = extract_products_payload(payload)
                open_rows: list[dict[str, Any]] = []
                for row in rows:
                    size = to_float(row.get("size"), 0.0)
                    if abs(size) <= 0:
                        continue
                    row_symbol = normalize_market_symbol(str(row.get("product_symbol") or row.get("symbol") or ""))
                    if symbol and row_symbol != symbol:
                        continue
                    open_rows.append(row)
                self._json(200, {
                    "ok": True, "mode": mode, "symbol": symbol,
                    "open": bool(open_rows),
                    "position": open_rows[0] if open_rows else None,
                    "positions": open_rows,
                })
                return

            if path == "/delta/orders_all":
                status, payload, raw = signed_delta_call(
                    key=key, secret=secret, mode=mode, method="DELETE", path="/v2/orders/all",
                )
                add_log("delta_orders_all", mode=mode, status=status, error=short_text(raw) if status >= 400 else "")
                if status >= 400:
                    self._json(status, {"ok": False, "error": best_error(payload, raw), "result": payload})
                    return
                self._json(200, {"ok": True, "mode": mode, "result": payload})
                return

            if path == "/delta/order":
                product_id_in = as_int_if_possible(body.get("product_id"))
                product_id = remap_product_id(mode, product_id_in)
                side = str(body.get("side") or "").strip().lower()
                size_raw = body.get("size")
                try:
                    size = int(round(float(size_raw)))
                except Exception:
                    size = None
                if product_id in ("", None) or side not in ("buy", "sell") or size in ("", None):
                    self._json(400, {"ok": False, "error": "Missing/invalid product_id, side, size"})
                    return
                if size <= 0:
                    self._json(400, {"ok": False, "error": "size must be positive integer lots"})
                    return
                remaining = cooldown_remaining(mode, key, product_id)
                if remaining > 0:
                    self._json(429, {
                        "ok": False,
                        "error": f"Cooldown active for {remaining}s",
                        "cooldown_remaining": remaining,
                        "product_id": product_id,
                    })
                    return
                order_type_in = str(body.get("order_type") or "market").strip().lower()
                order_type = "market_order" if order_type_in in ("market", "market_order") else "limit_order"
                payload_body: dict[str, Any] = {
                    "product_id": product_id,
                    "side": side,
                    "size": size,
                    "order_type": order_type,
                    "reduce_only": bool(body.get("reduce_only", False)),
                }
                if order_type == "limit_order" and body.get("limit_price") is not None:
                    payload_body["limit_price"] = body.get("limit_price")

                bracket_sl = to_float(body.get("bracket_stop_loss_price"), float("nan"))
                bracket_tp = to_float(body.get("bracket_take_profit_price"), float("nan"))
                bracket_trigger = str(body.get("bracket_stop_trigger_method") or "mark_price").strip()
                if math.isfinite(bracket_sl):
                    payload_body["bracket_stop_loss_price"] = bracket_sl
                if math.isfinite(bracket_tp):
                    payload_body["bracket_take_profit_price"] = bracket_tp
                if ("bracket_stop_loss_price" in payload_body or "bracket_take_profit_price" in payload_body) and bracket_trigger:
                    payload_body["bracket_stop_trigger_method"] = bracket_trigger

                status, payload, raw = signed_delta_call(
                    key=key, secret=secret, mode=mode, method="POST", path="/v2/orders", body=payload_body,
                )
                add_log("delta_order", mode=mode, status=status, side=side, product_id=product_id,
                        product_id_in=product_id_in, error=short_text(raw) if status >= 400 else "")
                if status >= 400:
                    self._json(status, {"ok": False, "error": best_error(payload, raw), "result": payload})
                    return
                cooldown_until = start_cooldown(mode, key, product_id)
                record_trade_event("order", mode, product_id, side=side, size=size, reduce_only=False)
                self._json(200, {
                    "ok": True,
                    "mode": mode,
                    "result": payload,
                    "cooldown_until": cooldown_until,
                    "cooldown_seconds": int(TRADE_COOLDOWN_SEC),
                })
                return

            if path == "/delta/close":
                product_id_in = as_int_if_possible(body.get("product_id"))
                product_id = remap_product_id(mode, product_id_in)
                current_side = str(body.get("side") or "").strip().lower()
                size_raw = body.get("size")
                try:
                    size = int(round(float(size_raw)))
                except Exception:
                    size = None
                if product_id in ("", None) or current_side not in ("buy", "sell") or size in ("", None):
                    self._json(400, {"ok": False, "error": "Missing/invalid product_id, side, size"})
                    return
                if size <= 0:
                    self._json(400, {"ok": False, "error": "size must be positive integer lots"})
                    return
                close_side = "sell" if current_side == "buy" else "buy"
                payload_body = {
                    "product_id": product_id,
                    "side": close_side,
                    "size": size,
                    "order_type": "market_order",
                    "reduce_only": True,
                }
                status, payload, raw = signed_delta_call(
                    key=key, secret=secret, mode=mode, method="POST", path="/v2/orders", body=payload_body,
                )
                add_log("delta_close", mode=mode, status=status, from_side=current_side, close_side=close_side,
                        product_id=product_id, product_id_in=product_id_in, error=short_text(raw) if status >= 400 else "")
                if status >= 400:
                    self._json(status, {"ok": False, "error": best_error(payload, raw), "result": payload})
                    return
                cooldown_until = start_cooldown(mode, key, product_id)
                record_trade_event("close", mode, product_id, side=close_side, size=size, reduce_only=True)
                self._json(200, {
                    "ok": True,
                    "mode": mode,
                    "result": payload,
                    "cooldown_until": cooldown_until,
                    "cooldown_seconds": int(TRADE_COOLDOWN_SEC),
                })
                return

        self._json(404, {"ok": False, "error": "Not found"})


################################################################################
# BACKGROUND BOT ENGINE â€” Full Pro MTF + NN logic (mirrors JS app exactly)
# Runs 24/7 even when browser is closed.
################################################################################

import threading
import math as _math

BOT_STATE: dict[str, Any] = {}
BOT_STATE_LOCK = threading.Lock()

DELTA_CONTRACT_VALUE: dict[str, float] = {
    "BTCUSDT": 0.001,
    "ETHUSDT": 0.01,
    "SOLUSDT": 1.0,
    "PAXGUSDT": 0.001,
}
TAKER_FEE = 0.0005
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "PAXGUSDT"]
SYMBOL_TO_MARKET = {
    "BTCUSDT": "BTCUSD",
    "ETHUSDT": "ETHUSD",
    "SOLUSDT": "SOLUSD",
    "PAXGUSDT": "PAXGUSD",
}
USD_TO_INR = 83.0
TF_SEC = {"15m": 900, "30m": 1800}
TF_LIST = ["15m", "30m"]


# â”€â”€ Candle helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def bot_fetch_candles(mode: str, symbol: str, tf: str = "15m", limit: int = 120) -> list[dict]:
    market_sym = SYMBOL_TO_MARKET.get(symbol, symbol.replace("USDT", "USD"))
    sec = DELTA_RESOLUTION_SECONDS.get(tf, 900)
    end_s = int(time.time())
    start_s = end_s - sec * (limit + 10)
    try:
        status, payload, _ = public_delta_call(
            mode, "/v2/history/candles",
            {"symbol": market_sym, "resolution": tf, "start": start_s, "end": end_s},
        )
        if status >= 400:
            return []
        rows = normalize_candles(extract_candles_payload(payload), sec)
        return rows[-limit:]
    except Exception:
        return []


# â”€â”€ Indicator library (mirrors JS worker) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _sma(arr: list[float], n: int) -> float | None:
    if len(arr) < n:
        return None
    return sum(arr[-n:]) / n

def ema(values: list[float], period: int) -> list[float]:
    if len(values) < period:
        return []
    k = 2.0 / (period + 1)
    e = sum(values[:period]) / period
    result = [e]
    for v in values[period:]:
        e = v * k + e * (1 - k)
        result.append(e)
    return result

def _ema_last(arr: list[float], n: int) -> float | None:
    vals = ema(arr, n)
    return vals[-1] if vals else None

def _rsi(closes: list[float], p: int = 14) -> float:
    if len(closes) < p + 1:
        return 50.0
    gains, losses = 0.0, 0.0
    for i in range(len(closes) - p, len(closes)):
        d = closes[i] - closes[i-1]
        if d > 0: gains += d
        else: losses -= d
    if losses == 0:
        return 100.0
    rs = gains / losses
    return 100 - (100 / (1 + rs))

def _bb(closes: list[float], p: int = 20):
    if len(closes) < p:
        return None
    m = sum(closes[-p:]) / p
    var = sum((c - m)**2 for c in closes[-p:]) / p
    sd = var**0.5
    return {"u": m + 2*sd, "m": m, "l": m - 2*sd}

def _atr(candles: list[dict], p: int = 14) -> float:
    if len(candles) < p + 1:
        return 0.0
    s = 0.0
    for i in range(len(candles) - p, len(candles)):
        c, prev = candles[i], candles[i-1]
        tr = max(
            float(c.get("high",0)) - float(c.get("low",0)),
            abs(float(c.get("high",0)) - float(prev.get("close",0))),
            abs(float(c.get("low",0)) - float(prev.get("close",0))),
        )
        s += tr
    return s / p

def _adx(candles: list[dict], period: int = 14):
    if len(candles) < period + 2:
        return {"adx": 0.0, "diPlus": 0.0, "diMinus": 0.0}
    trs, plus_dm, minus_dm = [], [], []
    for i in range(1, len(candles)):
        c, p = candles[i], candles[i-1]
        up = float(c.get("high",0)) - float(p.get("high",0))
        down = float(p.get("low",0)) - float(c.get("low",0))
        plus_dm.append(up if up > down and up > 0 else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)
        tr = max(
            float(c.get("high",0)) - float(c.get("low",0)),
            abs(float(c.get("high",0)) - float(p.get("close",0))),
            abs(float(c.get("low",0)) - float(p.get("close",0))),
        )
        trs.append(tr)
    def _slice_sum(arr): return sum(arr[-period:])
    tr_n = _slice_sum(trs)
    p_n = _slice_sum(plus_dm)
    m_n = _slice_sum(minus_dm)
    di_p = 100 * p_n / tr_n if tr_n > 0 else 0.0
    di_m = 100 * m_n / tr_n if tr_n > 0 else 0.0
    dx = 100 * abs(di_p - di_m) / (di_p + di_m) if (di_p + di_m) > 0 else 0.0
    dxs = []
    for i in range(period, len(trs)):
        tr = sum(trs[i-period:i])
        pp = sum(plus_dm[i-period:i])
        mm = sum(minus_dm[i-period:i])
        dip = 100 * pp / tr if tr > 0 else 0.0
        dim = 100 * mm / tr if tr > 0 else 0.0
        ddx = 100 * abs(dip - dim) / (dip + dim) if (dip + dim) > 0 else 0.0
        dxs.append(ddx)
    adx_v = sum(dxs[-period:]) / min(period, len(dxs)) if dxs else dx
    return {"adx": adx_v, "diPlus": di_p, "diMinus": di_m}


# â”€â”€ Signal computation (mirrors JS computeSignals) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def compute_signals(candles: list[dict]) -> dict:
    closes = [float(c.get("close", 0)) for c in candles]
    price = closes[-1] if closes else 0.0
    buy, sell = 0, 0

    ma7  = _sma(closes, 7) or price
    ma25 = _sma(closes, 25) or price
    ma99 = _sma(closes, 99) or price
    ema12 = _ema_last(closes, 12) or price
    ema26 = _ema_last(closes, 26) or price
    ema50 = _ema_last(closes, 50) or price
    ema200 = _ema_last(closes, 200) or price

    buy  += 15 if price > ma7 else 0;  sell += 15 if price < ma7 else 0
    buy  += 15 if price > ma25 else 0; sell += 15 if price < ma25 else 0
    buy  += 15 if price > ma99 else 0; sell += 15 if price < ma99 else 0
    buy  += 20 if ma7 > ma25 else 0;   sell += 20 if ma7 < ma25 else 0
    buy  += 15 if ma25 > ma99 else 0;  sell += 15 if ma25 < ma99 else 0
    buy  += 10 if price > ema12 else 0; sell += 10 if price < ema12 else 0
    buy  += 15 if ema12 > ema26 else 0; sell += 15 if ema12 < ema26 else 0

    rs = _rsi(closes, 14)
    buy  += 40 if rs < 30 else (25 if rs < 40 else (10 if rs < 50 else 0))
    sell += 40 if rs > 70 else (25 if rs > 60 else (10 if rs > 50 else 0))

    macd = ema12 - ema26
    buy  += 50 if macd > 0 else 0
    sell += 50 if macd < 0 else 0

    b = _bb(closes, 20)
    if b:
        buy  += 50 if price < b["l"] else (25 if price < b["m"] else 0)
        sell += 50 if price > b["u"] else (25 if price > b["m"] else 0)

    vols = [float(c.get("volume", 0)) for c in candles[-20:]]
    avg_vol = sum(vols) / max(1, len(vols))
    vol = float(candles[-1].get("volume", 0))
    if len(closes) >= 6:
        mom = (price - closes[-6]) / closes[-6] * 100 if closes[-6] else 0
    else:
        mom = 0.0
    buy  += 50 if (vol > avg_vol and mom > 0) else (25 if mom > 0.2 else 0)
    sell += 50 if (vol > avg_vol and mom < 0) else (25 if mom < -0.2 else 0)
    buy  += 30 if (price > ma7 and price > ma25 and mom > 0) else 0
    sell += 30 if (price < ma7 and price < ma25 and mom < 0) else 0

    atr_v = _atr(candles, 14)
    d = _adx(candles, 14)

    adx = d["adx"]
    regime = "--"
    if ema50 and ema200:
        if adx <= 18:
            regime = "SIDEWAYS"
        elif price > ema200 and ema50 > ema200:
            regime = "BULL"
        elif price < ema200 and ema50 < ema200:
            regime = "BEAR"
        else:
            regime = "SIDEWAYS"

    return {
        "buy": buy, "sell": sell, "atr": atr_v,
        "price": price, "ema50": ema50, "ema200": ema200,
        "adx": adx, "diP": d["diPlus"], "diM": d["diMinus"],
        "regime": regime,
    }


# â”€â”€ NN predict (mirrors JS nnPredict) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def nn_predict(candles: list[dict]) -> dict:
    if len(candles) < 30:
        return {"dir": "UP", "conf": 55, "pat": "Warmup"}
    last = candles[-1]
    prev = candles[-2]
    prev2 = candles[-3]
    closes = [float(c.get("close", 0)) for c in candles]

    vols = [float(c.get("volume", 0)) for c in candles[-20:]]
    avg_vol = sum(vols) / max(1, len(vols))
    vol_ratio = float(last.get("volume", 0)) / max(1e-9, avg_vol)

    close_l = float(last.get("close", 0))
    open_l  = float(last.get("open", 0))
    body_pct = abs(close_l - open_l) / max(1e-9, open_l) * 100

    mom = (closes[-1] - closes[-6]) / max(1e-9, closes[-6]) * 100 if len(closes) >= 6 else 0.0

    is_g = close_l > open_l
    three_g = is_g and float(prev.get("close",0)) > float(prev.get("open",0)) and float(prev2.get("close",0)) > float(prev2.get("open",0))
    three_r = (not is_g) and float(prev.get("close",0)) < float(prev.get("open",0)) and float(prev2.get("close",0)) < float(prev2.get("open",0))

    vol_boost  = max(0, min(18, (vol_ratio - 1) * 30))
    body_boost = max(0, min(14, body_pct * 35))
    mom_boost  = max(0, min(22, abs(mom) * 35))
    high_l = float(last.get("high", close_l))
    low_l  = float(last.get("low", close_l))
    wick = abs(close_l - open_l) / max(1e-9, high_l - low_l) if high_l != low_l else 0
    wick_boost = max(0, min(8, (wick - 0.45) * 20))

    up_score = 50.0
    dn_score = 50.0

    if is_g: up_score += 5
    else: dn_score += 5

    if mom > 0: up_score += mom_boost
    else: dn_score += mom_boost

    if vol_ratio > 1.0:
        if is_g: up_score += vol_boost
        else: dn_score += vol_boost

    if body_pct > 0.12:
        if is_g: up_score += body_boost
        else: dn_score += body_boost

    if three_g: up_score += 7
    if three_r: dn_score += 7

    if is_g: up_score += wick_boost
    else: dn_score += wick_boost

    jitter = (abs(close_l * 1000) % 100) / 100
    up_score += jitter
    dn_score += (1 - jitter) * 0.25

    direction = "UP" if up_score >= dn_score else "DOWN"
    conf = max(50, min(99, round(max(up_score, dn_score))))

    pat = "Flow"
    if vol_ratio >= 1.4:
        pat = "Vol Spike+Green" if is_g else "Vol Spike+Red"
    elif abs(mom) >= 0.25:
        pat = "+Momentum" if mom > 0 else "-Momentum"
    elif body_pct >= 0.25:
        pat = "Strong Body"
    elif three_g or three_r:
        pat = "3 Green" if three_g else "3 Red"

    return {"dir": direction, "conf": conf, "pat": pat}


# â”€â”€ Regime + filters (mirrors JS) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def determine_regime(sig30: dict, strict: bool = False) -> dict:
    adx = sig30.get("adx", 0)
    adx_min = 18 if strict else 12
    if adx <= adx_min:
        return {"regime": "SIDEWAYS", "ok": True, "side": None, "soft": True}
    r = sig30.get("regime", "--")
    if r == "BULL":
        return {"regime": "BULL", "ok": True, "side": "LONG", "soft": False}
    if r == "BEAR":
        return {"regime": "BEAR", "ok": True, "side": "SHORT", "soft": False}
    return {"regime": "SIDEWAYS", "ok": True, "side": None, "soft": True}

def no_trade_filters(candles15: list[dict], sig15: dict, strict: bool = False) -> dict:
    atr_min = 0.10 if strict else 0.06
    price = sig15.get("price", 0)
    atr = sig15.get("atr", 0)
    if price > 0 and atr > 0:
        atr_pct = atr / price * 100
        if atr_pct < atr_min:
            return {"ok": False, "why": "Low ATR (chop)"}
    if len(candles15) < 30:
        return {"ok": False, "why": "15m warmup"}
    last = candles15[-1]
    r = float(last.get("high",0)) - float(last.get("low",0))
    if atr > 0 and r > atr * 2.8:
        return {"ok": False, "why": "Spike candle"}
    body = abs(float(last.get("close",0)) - float(last.get("open",0)))
    wick_top = float(last.get("high",0)) - max(float(last.get("close",0)), float(last.get("open",0)))
    wick_bot = min(float(last.get("close",0)), float(last.get("open",0))) - float(last.get("low",0))
    if body > 0 and (wick_top + wick_bot) > body * 4:
        return {"ok": False, "why": "Wicky chop"}
    ranges = [float(c.get("high",0)) - float(c.get("low",0)) for c in candles15[-10:]]
    avg_r = sum(ranges) / max(1, len(ranges))
    if atr > 0 and avg_r < atr * 0.55:
        return {"ok": False, "why": "Tight range"}
    return {"ok": True}

def setup_signal(nn15: dict, strict: bool = False) -> dict:
    conf_min = 60 if strict else 50
    if nn15.get("conf", 0) < conf_min:
        return {"ok": False, "why": "NN15 low conf"}
    direction = "LONG" if nn15.get("dir") == "UP" else "SHORT"
    return {"ok": True, "dir": direction, "conf": nn15.get("conf", 0)}

def trigger_signal(sig15: dict, want_side: str, strict: bool = False) -> dict:
    diff_min = 35 if strict else 20
    diff = abs(sig15.get("buy", 0) - sig15.get("sell", 0))
    if diff < diff_min:
        return {"ok": False, "why": "Weak diff"}
    if want_side == "LONG" and sig15.get("buy", 0) <= sig15.get("sell", 0):
        return {"ok": False, "why": "15m not bullish"}
    if want_side == "SHORT" and sig15.get("sell", 0) <= sig15.get("buy", 0):
        return {"ok": False, "why": "15m not bearish"}
    return {"ok": True}

def execution_signal(candles15: list[dict], want_side: str) -> dict:
    if len(candles15) < 30:
        return {"ok": False, "why": "No 15m"}
    last = candles15[-1]
    is_green = float(last.get("close",0)) > float(last.get("open",0))
    vols = [float(c.get("volume",0)) for c in candles15[-20:]]
    avg_vol = sum(vols) / max(1, len(vols))
    vol_ok = float(last.get("volume",0)) > avg_vol
    if want_side == "LONG" and (not is_green or not vol_ok):
        return {"ok": False, "why": "15m no confirmation"}
    if want_side == "SHORT" and (is_green or not vol_ok):
        return {"ok": False, "why": "15m no confirmation"}
    return {"ok": True}


# â”€â”€ SL/TP (mirrors JS slTpAdvanced) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def rr_by_tf(tf: str, adx: float) -> float:
    rr = 3.2 if tf == "15m" else 4.6
    if adx > 25: rr *= 1.15
    if adx > 35: rr *= 1.10
    return min(8.0, max(1.2, rr))

def bot_sl_tp(symbol: str, side: str, entry: float,
               sl_points: float = 0, tp_points: float = 0,
               tf: str = "15m", atr: float = 0.0, adx: float = 0.0) -> tuple[float, float]:
    is_long = side == "LONG"
    if sl_points > 0 and tp_points > 0:
        sl = entry - sl_points if is_long else entry + sl_points
        tp = entry + tp_points if is_long else entry - tp_points
        return sl, tp
    if symbol == "ETHUSDT":
        dist = 10.0
        return (entry - dist, entry + dist) if is_long else (entry + dist, entry - dist)
    if symbol == "SOLUSDT":
        sl_dist = 60.0 / USD_TO_INR
        rr = max(2.0, rr_by_tf(tf, adx))
        sl = entry - sl_dist if is_long else entry + sl_dist
        tp = entry + sl_dist * rr if is_long else entry - sl_dist * rr
        return sl, tp
    if atr > 0:
        tf_mult = 2.05 if tf == "15m" else 2.90
        sl_dist = atr * tf_mult
        rr = rr_by_tf(tf, adx)
        if tf == "30m" and adx > 28: rr = min(8.0, rr + 0.8)
        sl = entry - sl_dist if is_long else entry + sl_dist
        tp = entry + sl_dist * rr if is_long else entry - sl_dist * rr
        return sl, tp
    # fallback
    defaults = {"BTCUSDT": (500.0, 500.0), "PAXGUSDT": (5.0, 5.0)}
    d_sl, d_tp = defaults.get(symbol, (entry * 0.005, entry * 0.005))
    sl = entry - d_sl if is_long else entry + d_sl
    tp = entry + d_tp if is_long else entry - d_tp
    return sl, tp


# â”€â”€ Live price / position helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def bot_get_price(mode: str, symbol: str) -> float | None:
    market_sym = SYMBOL_TO_MARKET.get(symbol, symbol.replace("USDT", "USD"))
    try:
        status, payload, _ = public_delta_call(
            mode, "/v2/tickers", {"contract_types": "perpetual_futures"}
        )
        if status >= 400:
            return None
        tickers = extract_tickers_payload(payload)
        for t in tickers:
            if str(t.get("symbol") or "").upper() == market_sym:
                p = to_float(t.get("mark_price"), 0.0)
                if p > 0: return p
                p = to_float(t.get("close"), 0.0)
                if p > 0: return p
    except Exception:
        pass
    return None

def bot_check_position_open(key: str, secret: str, mode: str, symbol: str) -> bool:
    market_sym = SYMBOL_TO_MARKET.get(symbol, symbol.replace("USDT", "USD"))
    try:
        status, payload, _ = signed_delta_call(
            key=key, secret=secret, mode=mode, method="GET", path="/v2/positions"
        )
        if status >= 400:
            return False
        rows = extract_products_payload(payload)
        for row in rows:
            size = to_float(row.get("size"), 0.0)
            sym = str(row.get("product_symbol") or row.get("symbol") or "").upper()
            if abs(size) > 0 and (sym == market_sym or sym == symbol):
                return True
    except Exception:
        pass
    return False

def bot_calc_fee(entry: float, exit_price: float, qty: float) -> float:
    return (entry + exit_price) * qty * TAKER_FEE

def bot_close_trade_on_exchange(key: str, secret: str, mode: str,
                                 product_id: Any, side: str, size: int) -> bool:
    close_side = "sell" if side == "buy" else "buy"
    try:
        status, _, _ = signed_delta_call(
            key=key, secret=secret, mode=mode, method="POST", path="/v2/orders",
            body={"product_id": remap_product_id(mode, product_id),
                  "side": close_side, "size": size,
                  "order_type": "market_order", "reduce_only": True},
        )
        return status < 400
    except Exception:
        return False

def bot_place_order(key: str, secret: str, mode: str, product_id: Any,
                    side: str, size: int, sl: float | None, tp: float | None) -> bool:
    body: dict[str, Any] = {
        "product_id": remap_product_id(mode, product_id),
        "side": side, "size": size,
        "order_type": "market_order", "reduce_only": False,
    }
    if sl is not None:
        body["bracket_stop_loss_price"] = str(round(sl, 6))
        body["bracket_stop_trigger_method"] = "mark_price"
    if tp is not None:
        body["bracket_take_profit_price"] = str(round(tp, 6))
    try:
        status, _, _ = signed_delta_call(
            key=key, secret=secret, mode=mode, method="POST", path="/v2/orders", body=body
        )
        return status < 400
    except Exception:
        return False


# â”€â”€ Per-candle close tracking â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def get_last_closed_candle(candles: list[dict], tf: str) -> int:
    """Return the timestamp of the last *closed* candle (not the live one)."""
    if not candles:
        return 0
    sec = TF_SEC.get(tf, 900)
    now_bucket = (int(time.time()) // sec) * sec
    arr = candles
    last = arr[-1]
    t = int(last.get("time", 0))
    if t < now_bucket:
        return t
    if len(arr) >= 2:
        return int(arr[-2].get("time", 0))
    return 0


def bot_daily_counter(state: dict[str, Any]) -> dict[str, Any]:
    c = state.setdefault("daily_trade_counter", {"day": "", "count": 0})
    day = utc_day_key()
    if str(c.get("day") or "") != day:
        c["day"] = day
        c["count"] = 0
    try:
        c["count"] = max(0, int(c.get("count", 0)))
    except Exception:
        c["count"] = 0
    return c


def bot_can_open_today(state: dict[str, Any]) -> bool:
    c = bot_daily_counter(state)
    try:
        limit = max(1, min(10, int(state.get("daily_trade_limit", 10))))
    except Exception:
        limit = 10
    return int(c.get("count", 0)) < limit


def bot_mark_open_today(state: dict[str, Any]) -> None:
    c = bot_daily_counter(state)
    c["count"] = int(c.get("count", 0)) + 1


def bot_price_action_signal(candles_15: list[dict], sig15: dict, regime: dict) -> dict[str, Any]:
    if len(candles_15) < 24:
        return {"ok": False, "why": "warmup"}
    last = candles_15[-1]
    prev = candles_15[-2]
    highs = [to_float(c.get("high"), 0.0) for c in candles_15[-13:-1]]
    lows = [to_float(c.get("low"), 0.0) for c in candles_15[-13:-1]]
    if not highs or not lows:
        return {"ok": False, "why": "range"}
    swing_high = max(highs)
    swing_low = min(lows)
    vols = [to_float(c.get("volume"), 0.0) for c in candles_15[-20:]]
    avg_vol = sum(vols) / max(1, len(vols))
    vol_boost = to_float(last.get("volume"), 0.0) / max(1e-9, avg_vol)
    rng = max(1e-9, to_float(last.get("high"), 0.0) - to_float(last.get("low"), 0.0))
    body = abs(to_float(last.get("close"), 0.0) - to_float(last.get("open"), 0.0))
    body_power = body / rng
    adx = to_float(sig15.get("adx"), 0.0)
    if adx < 16 or vol_boost < 1.1 or body_power < 0.55:
        return {"ok": False, "why": "weak"}
    up_break = to_float(last.get("close"), 0.0) > swing_high and to_float(last.get("close"), 0.0) > to_float(prev.get("high"), 0.0)
    dn_break = to_float(last.get("close"), 0.0) < swing_low and to_float(last.get("close"), 0.0) < to_float(prev.get("low"), 0.0)
    side = "LONG" if up_break else ("SHORT" if dn_break else None)
    if side is None:
        return {"ok": False, "why": "no_breakout"}
    if regime.get("ok") and not regime.get("soft") and regime.get("side") and regime.get("side") != side:
        return {"ok": False, "why": "regime_mismatch"}
    quality = round(max(50, min(99, 58 + adx + (vol_boost * 10) + (body_power * 12))))
    return {"ok": True, "side": side, "quality": quality}


def bot_combined_signal(sig15: dict, sig30: dict, regime: dict, pa: dict, news: dict) -> dict[str, Any]:
    news_bias = str(news.get("bias") or "neutral").lower()
    news_score = abs(to_float(news.get("score"), 0.0))
    news_side = "LONG" if news_bias == "bullish" else ("SHORT" if news_bias == "bearish" else None)
    regime_side = None if regime.get("soft") else regime.get("side")

    side: str | None = None
    quality = 0.0
    reason = ""

    if pa.get("ok"):
        side = str(pa.get("side"))
        quality = float(pa.get("quality", 0))
        reason = "PA breakout"
        if news_side == side and news_score >= 6:
            quality += min(18.0, news_score)
            reason += " + news aligned"

    if news_side and news_score >= 18 and (not regime_side or regime_side == news_side):
        news_quality = min(99.0, 76.0 + news_score)
        if side is None or news_quality > quality:
            side = news_side
            quality = news_quality
            reason = f"strong {news_bias} news"

    if side is None:
        return {"ok": False, "why": "no_combined_signal"}
    if regime_side and regime_side != side:
        return {"ok": False, "why": "30m_trend_mismatch"}

    buy = to_float(sig15.get("buy"), 0.0)
    sell = to_float(sig15.get("sell"), 0.0)
    diff = abs(buy - sell)
    trend_ok = buy >= sell if side == "LONG" else sell >= buy
    if not trend_ok and news_score < 24:
        return {"ok": False, "why": "15m_score_mismatch"}
    quality += min(10.0, diff / 4.0)

    quality_i = round(min(99.0, quality))
    return {
        "ok": quality_i >= 75,
        "side": side,
        "quality": quality_i,
        "strategy": f"Combined PA + News ({reason})",
        "why": reason,
    }


# â”€â”€ Core bot logic per symbol â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def bot_run_symbol(state: dict, symbol: str) -> None:
    key: str = state["key"]
    secret: str = state["secret"]
    mode: str = state["mode"]
    product_map: dict = state.get("product_map", {})
    lot: int = max(1, int(state.get("lot", 1)))
    sl_points: float = float(state.get("sl_points", 0))
    tp_points: float = float(state.get("tp_points", 0))
    bot_enabled: bool = bool(state.get("bot_enabled", False))
    live_trading: bool = bool(state.get("live_trading", False))
    strict: bool = bool(state.get("strict_mode", False))
    strategy_nn: bool = bool(state.get("strategy_nn", False))
    strategy_pa: bool = bool(state.get("strategy_pa", True))
    strategy_news: bool = bool(state.get("strategy_news", True))
    min_entry_gap_sec: int = max(60, int(state.get("min_entry_gap_sec", 1800)))

    pid_raw = product_map.get(symbol, "")
    if not pid_raw:
        return
    product_id = as_int_if_possible(pid_raw)

    trades: list[dict] = state.setdefault("trades", [])
    active = next((t for t in trades if t.get("symbol") == symbol and not t.get("closed")), None)

    price = bot_get_price(mode, symbol)
    if price is None or price <= 0:
        return

    contract_qty = DELTA_CONTRACT_VALUE.get(symbol, 0.01)
    qty = lot * contract_qty

    if active:
        is_long = active.get("side") == "LONG"
        sl = float(active.get("sl", 0))
        tp = float(active.get("tp", 0))
        reason = None

        if sl > 0 and tp > 0:
            if is_long:
                if price <= sl:
                    reason = "SL Hit"
                elif price >= tp:
                    reason = "TP Hit"
            else:
                if price >= sl:
                    reason = "SL Hit"
                elif price <= tp:
                    reason = "TP Hit"

        if reason:
            entry_p = float(active.get("entry", price))
            raw_pnl = (price - entry_p) * qty if is_long else (entry_p - price) * qty
            fee = bot_calc_fee(entry_p, price, qty)
            net_pnl = raw_pnl - fee

            if live_trading:
                exc_side = "buy" if active.get("side") == "LONG" else "sell"
                bot_close_trade_on_exchange(key, secret, mode, product_id, exc_side, lot)

            active["closed"] = True
            active["exit"] = price
            active["exit_time"] = utc_now()
            active["reason"] = reason
            active["pnl_usd"] = round(net_pnl, 4)
            active["fee_paid"] = round(fee, 4)

            current_balance = float(state.get("demo_balance", 1000.0))
            state["demo_balance"] = max(0.0, current_balance + net_pnl)

            record_trade_event(reason, mode, product_id, side=active.get("side"), symbol=symbol, pnl_usd=round(net_pnl, 4))
            add_log("bot_close", symbol=symbol, reason=reason, price=price, pnl=round(net_pnl, 4))
            state["last_close_time"] = time.time()
            trades[:] = [t for t in trades if not t.get("closed")]
            return

    last_close = float(state.get("last_close_time", 0))
    if time.time() - last_close < TRADE_COOLDOWN_SEC:
        return

    if not bot_enabled or active:
        return
    if not bot_can_open_today(state):
        return
    last_entry_time = float(state.get(f"last_entry_time_{symbol}", 0.0))
    if (time.time() - last_entry_time) < min_entry_gap_sec:
        return

    candles_15 = bot_fetch_candles(mode, symbol, "15m", 120)
    candles_30 = bot_fetch_candles(mode, symbol, "30m", 120)
    if len(candles_15) < 60 or len(candles_30) < 60:
        return

    last_closed_15 = get_last_closed_candle(candles_15, "15m")
    last_entry_candle = int(state.get(f"last_entry_candle_{symbol}", 0))
    if last_closed_15 == 0 or last_closed_15 == last_entry_candle:
        return

    sig15 = compute_signals(candles_15)
    sig30 = compute_signals(candles_30)
    regime = determine_regime(sig30, strict)
    pa = bot_price_action_signal(candles_15, sig15, regime)
    news = fetch_news_sentiment(symbol) if strategy_news else {"bias": "neutral", "score": 0}
    combined = bot_combined_signal(sig15, sig30, regime, pa if strategy_pa else {}, news)
    if not combined.get("ok"):
        return
    side = str(combined.get("side"))
    strategy = str(combined.get("strategy") or "Combined PA + News")
    if str(combined.get("why") or "").startswith("strong"):
        exc = {"ok": True}
    else:
        exc = execution_signal(candles_15, side)
    if not exc.get("ok"):
        return

    if live_trading and bot_check_position_open(key, secret, mode, symbol):
        return

    entry = price
    sl, tp = bot_sl_tp(symbol, side, entry, sl_points, tp_points, tf="15m", atr=sig15.get("atr", 0), adx=sig15.get("adx", 0))

    if live_trading:
        exc_side = "buy" if side == "LONG" else "sell"
        ok = bot_place_order(key, secret, mode, product_id, exc_side, lot, sl, tp)
        if not ok:
            add_log("bot_order_failed", symbol=symbol, side=side)
            return

    new_trade = {
        "id": int(time.time() * 1000),
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "lot": lot,
        "qty": qty,
        "entry_time": utc_now(),
        "closed": False,
        "strategy": strategy,
        "tf": "15m",
    }
    trades.append(new_trade)
    bot_mark_open_today(state)
    state["last_trade_time"] = time.time()
    state[f"last_entry_time_{symbol}"] = time.time()
    state[f"last_entry_candle_{symbol}"] = last_closed_15
    add_log("bot_open", symbol=symbol, side=side, entry=entry, sl=sl, tp=tp, strategy=strategy)


def bot_loop() -> None:
    """Main background bot loop â€” runs every 10 seconds."""
    add_log("bot_engine_started")
    while True:
        try:
            with BOT_STATE_LOCK:
                actors = list(BOT_STATE.items())
            for actor_id, state in actors:
                if not state.get("active"):
                    continue
                symbols_to_trade: list[str] = state.get("symbols", SYMBOLS)
                for symbol in symbols_to_trade:
                    try:
                        bot_run_symbol(state, symbol)
                    except Exception as e:
                        add_log("bot_symbol_error", symbol=symbol, error=str(e))
        except Exception as e:
            add_log("bot_loop_error", error=str(e))
        time.sleep(10)


_orig_do_GET = Handler.do_GET
_orig_do_POST = Handler.do_POST


def _patched_do_GET(self: Handler) -> None:
    parsed = urlparse(self.path)
    path = parsed.path
    q = parse_qs(parsed.query)

    if path == "/bot/status":
        with BOT_STATE_LOCK:
            actors = []
            for actor_id, state in BOT_STATE.items():
                if not state.get("active"):
                    continue
                actors.append({
                    "actor": actor_id,
                    "mode": state.get("mode"),
                    "bot_enabled": state.get("bot_enabled", False),
                    "live_trading": state.get("live_trading", False),
                    "symbols": state.get("symbols", SYMBOLS),
                    "demo_balance": state.get("demo_balance", 1000.0),
                    "trades": [t for t in state.get("trades", []) if not t.get("closed")],
                    "daily_trade_counter": bot_daily_counter(state),
                    "daily_trade_limit": max(1, min(10, int(state.get("daily_trade_limit", 10)))),
                    "last_heartbeat": state.get("last_heartbeat", 0),
                })
        self._json(200, {"ok": True, "bots": actors, "ts": utc_now()})
        return

    _orig_do_GET(self)


def _patched_do_POST(self: Handler) -> None:
    parsed = urlparse(self.path)
    path = parsed.path

    if path == "/bot/sync":
        """App syncs its state to the backend bot engine."""
        body = self._read_body_json()
        session = self._require_auth()
        if session is None:
            return

        key = str(body.get("key") or "").strip()
        secret = str(body.get("secret") or "").strip()
        mode = normalize_mode(str(body.get("mode") or "live"))
        if not key or not secret:
            saved_key, saved_secret = get_saved_creds(mode)
            key = key or saved_key
            secret = secret or saved_secret
        if not key or not secret:
            self._json(400, {"ok": False, "error": "Missing key/secret"})
            return
        save_creds(mode, key, secret)
        ak = actor_key(mode, key)

        with BOT_STATE_LOCK:
            existing = BOT_STATE.get(ak, {})
            # Merge incoming state with existing (preserve open trades)
            existing_trades = existing.get("trades", [])

            BOT_STATE[ak] = {
                **existing,
                "key": key,
                "secret": secret,
                "mode": mode,
                "active": True,
                # Preserve bot_enabled if app does not explicitly send it (keeps bot running after app closes)
                "bot_enabled": bool(body["bot_enabled"]) if "bot_enabled" in body else existing.get("bot_enabled", False),
                "live_trading": bool(body["live_trading"]) if "live_trading" in body else existing.get("live_trading", False),
                "product_map": dict(body.get("product_map") or {}),
                "lot": max(1, int(body.get("lot", 1))),
                "sl_points": float(body.get("sl_points", 0)),
                "tp_points": float(body.get("tp_points", 0)),
                "strategy_nn": bool(body["strategy_nn"]) if "strategy_nn" in body else existing.get("strategy_nn", False),
                "strategy_pa": bool(body["strategy_pa"]) if "strategy_pa" in body else existing.get("strategy_pa", True),
                "strategy_news": bool(body["strategy_news"]) if "strategy_news" in body else existing.get("strategy_news", True),
                "daily_trade_limit": max(1, min(10, int(body.get("daily_trade_limit", existing.get("daily_trade_limit", 10))))),
                "min_entry_gap_sec": max(60, int(body.get("min_entry_gap_sec", existing.get("min_entry_gap_sec", 1800)))),
                "daily_trade_counter": existing.get("daily_trade_counter", {"day": "", "count": 0}),
                "symbols": list(body.get("symbols") or SYMBOLS),
                "demo_balance": float(body.get("demo_balance") or
                                      existing.get("demo_balance", 1000.0)),
                "trades": existing_trades,  # keep backend's own trade list
                "last_heartbeat": time.time(),
            }
            current_state = BOT_STATE[ak]

        # Return current bot state back to app (trades + balance)
        open_trades = [t for t in current_state.get("trades", []) if not t.get("closed")]
        self._json(200, {
            "ok": True,
            "mode": mode,
            "bot_enabled": current_state["bot_enabled"],
            "live_trading": current_state["live_trading"],
            "demo_balance": current_state.get("demo_balance", 1000.0),
            "trades": open_trades,
            "ts": utc_now(),
        })
        return

    if path == "/bot/stop":
        """Deactivate bot for this key."""
        body = self._read_body_json()
        session = self._require_auth()
        if session is None:
            return
        key = str(body.get("key") or "").strip()
        secret = str(body.get("secret") or "").strip()
        mode = normalize_mode(str(body.get("mode") or "live"))
        if not key or not secret:
            saved_key, saved_secret = get_saved_creds(mode)
            key = key or saved_key
            secret = secret or saved_secret
        if not key:
            self._json(400, {"ok": False, "error": "Missing key"})
            return
        ak = actor_key(mode, key)
        with BOT_STATE_LOCK:
            if ak in BOT_STATE:
                BOT_STATE[ak]["active"] = False
                BOT_STATE[ak]["bot_enabled"] = False
        self._json(200, {"ok": True})
        return

    _orig_do_POST(self)


Handler.do_GET = _patched_do_GET
Handler.do_POST = _patched_do_POST


def main() -> None:
    parser = argparse.ArgumentParser(description="Delta India local backend")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind (default: 0.0.0.0)")
    parser.add_argument("--port", default=7860, type=int, help="Port to bind (default: 7860)")
    args = parser.parse_args()

    # Start background bot engine thread
    bot_thread = threading.Thread(target=bot_loop, daemon=True, name="BotEngine")
    bot_thread.start()

    add_log("startup", host=args.host, port=args.port,
            live_base=DELTA_BASE_LIVE, demo_base=DELTA_BASE_DEMO)
    with ThreadingHTTPServer((args.host, args.port), Handler) as httpd:
        print(f"Delta backend running on http://{args.host}:{args.port}")
        print(f"App UI: http://{args.host}:{args.port}/")
        print(f"Bot engine running in background thread.")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            add_log("shutdown")


if __name__ == "__main__":
    main()

