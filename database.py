import requests
import hashlib
import streamlit as st
from collections import Counter
import socket

_FALLBACK_URL = "https://kgeqtguypsfrhryxbrfu.supabase.co"
_FALLBACK_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImtnZXF0Z3V5cHNmcmhyeXhicmZ1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODAzNjk0NTIsImV4cCI6MjA5NTk0NTQ1Mn0.oIPcHvCu9PQKbd1VFXH5jP8Zkr85IzDKi6sEWWyHNFg"

DB_ERROR_MSG = "⚠️ 데이터베이스 연결 실패 — 잠시 후 다시 시도해주세요."


def _base_url() -> str:
    try:
        val = st.secrets.get("SUPABASE_URL", "") or ""
        val = val.strip().strip('"').strip("'")
        return val if val.startswith("http") else _FALLBACK_URL
    except Exception:
        return _FALLBACK_URL


def _api_key() -> str:
    try:
        val = st.secrets.get("SUPABASE_KEY", "") or ""
        val = val.strip().strip('"').strip("'")
        return val if len(val) > 20 else _FALLBACK_KEY
    except Exception:
        return _FALLBACK_KEY


def _headers(prefer: str = "") -> dict:
    key = _api_key()
    h = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    if prefer:
        h["Prefer"] = prefer
    return h


def _rest(table: str) -> str:
    return f"{_base_url()}/rest/v1/{table}"


def hash_pw(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()


def check_connection() -> str:
    """진단용: 실제 연결 에러 메시지 반환"""
    host = _base_url().replace("https://", "").replace("http://", "").split("/")[0]
    # 1. DNS 확인
    try:
        ip = socket.gethostbyname(host)
    except socket.gaierror as e:
        return f"❌ DNS 실패: {host} → {e}"
    # 2. HTTP 연결 확인
    try:
        r = requests.get(
            _rest("users"),
            headers=_headers(),
            params={"select": "id", "limit": "1"},
            timeout=10,
            verify=False,  # SSL 우회 테스트
        )
        return f"✅ 연결 성공 (SSL 우회): HTTP {r.status_code} — IP={ip}"
    except Exception as e:
        pass
    try:
        r = requests.get(
            _rest("users"),
            headers=_headers(),
            params={"select": "id", "limit": "1"},
            timeout=10,
        )
        return f"✅ 연결 성공: HTTP {r.status_code} — IP={ip}"
    except Exception as e:
        return f"❌ HTTP 실패: {type(e).__name__}: {str(e)[:300]}"


# ── 사용자 ──────────────────────────────────────────────────────────────
def signup(username: str, password: str) -> dict:
    try:
        r = requests.get(
            _rest("users"),
            headers=_headers(),
            params={"select": "id", "username": f"eq.{username}"},
            timeout=10,
        )
        if r.ok and r.json():
            return {"ok": False, "msg": "이미 사용 중인 아이디예요."}
        r2 = requests.post(
            _rest("users"),
            headers=_headers("return=minimal"),
            json={"username": username, "password": hash_pw(password)},
            timeout=10,
        )
        if r2.status_code in (200, 201, 204):
            return {"ok": True}
        return {"ok": False, "msg": f"가입 실패: {r2.text}"}
    except Exception as e:
        return {"ok": False, "msg": f"{DB_ERROR_MSG} ({type(e).__name__})"}


def login(username: str, password: str) -> dict:
    try:
        r = requests.get(
            _rest("users"),
            headers=_headers(),
            params={
                "select": "*",
                "username": f"eq.{username}",
                "password": f"eq.{hash_pw(password)}",
            },
            timeout=10,
        )
        if r.ok and r.json():
            return {"ok": True, "user": r.json()[0]}
        return {"ok": False, "msg": "아이디 또는 비밀번호가 틀렸어요."}
    except Exception as e:
        return {"ok": False, "msg": f"{DB_ERROR_MSG} ({type(e).__name__})"}


# ── 질문 로그 ────────────────────────────────────────────────────────────
def log_query(username: str, query: str, item: str = None):
    try:
        body = {"username": username, "query_text": query}
        if item:
            body["item"] = item
        requests.post(
            _rest("query_logs"),
            headers=_headers("return=minimal"),
            json=body,
            timeout=5,
        )
    except Exception:
        pass


def get_history(username: str, limit: int = 30):
    try:
        r = requests.get(
            _rest("query_logs"),
            headers=_headers(),
            params={
                "select": "*",
                "username": f"eq.{username}",
                "order": "created_at.desc",
                "limit": limit,
            },
            timeout=10,
        )
        return r.json() if r.ok else []
    except Exception:
        return []


# ── 랭킹 ─────────────────────────────────────────────────────────────────
def get_item_ranking(top_n: int = 10):
    try:
        r = requests.get(
            _rest("query_logs"),
            headers=_headers(),
            params={"select": "item"},
            timeout=10,
        )
        rows = r.json() if r.ok else []
        counts = Counter(row["item"] for row in rows if row.get("item"))
        return counts.most_common(top_n)
    except Exception:
        return []


def get_query_ranking(top_n: int = 10):
    try:
        r = requests.get(
            _rest("query_logs"),
            headers=_headers(),
            params={"select": "query_text"},
            timeout=10,
        )
        rows = r.json() if r.ok else []
        counts = Counter(row["query_text"] for row in rows if row.get("query_text"))
        return counts.most_common(top_n)
    except Exception:
        return []


def get_total_stats():
    try:
        r1 = requests.get(_rest("query_logs"), headers=_headers(), params={"select": "id"}, timeout=10)
        r2 = requests.get(_rest("users"), headers=_headers(), params={"select": "id"}, timeout=10)
        total = len(r1.json()) if r1.ok else 0
        users = len(r2.json()) if r2.ok else 0
        return total, users
    except Exception:
        return 0, 0


# ── 게시판 ───────────────────────────────────────────────────────────────
def get_posts():
    try:
        r = requests.get(
            _rest("posts"),
            headers=_headers(),
            params={"select": "*", "order": "created_at.desc"},
            timeout=10,
        )
        return r.json() if r.ok else []
    except Exception:
        return []


def create_post(username: str, title: str, content: str):
    try:
        requests.post(
            _rest("posts"),
            headers=_headers("return=minimal"),
            json={"username": username, "title": title, "content": content},
            timeout=10,
        )
    except Exception:
        pass


def delete_post(post_id: str, username: str):
    try:
        requests.delete(
            _rest("posts"),
            headers=_headers(),
            params={"id": f"eq.{post_id}", "username": f"eq.{username}"},
            timeout=10,
        )
    except Exception:
        pass


def get_comments(post_id: str):
    try:
        r = requests.get(
            _rest("comments"),
            headers=_headers(),
            params={"select": "*", "post_id": f"eq.{post_id}", "order": "created_at.asc"},
            timeout=10,
        )
        return r.json() if r.ok else []
    except Exception:
        return []


def create_comment(post_id: str, username: str, content: str):
    try:
        requests.post(
            _rest("comments"),
            headers=_headers("return=minimal"),
            json={"post_id": post_id, "username": username, "content": content},
            timeout=10,
        )
    except Exception:
        pass


def delete_comment(comment_id: str, username: str):
    try:
        requests.delete(
            _rest("comments"),
            headers=_headers(),
            params={"id": f"eq.{comment_id}", "username": f"eq.{username}"},
            timeout=10,
        )
    except Exception:
        pass
