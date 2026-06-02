import requests
import hashlib
import streamlit as st
from collections import Counter

_FALLBACK_URL = "https://kgeqtguypsfrhryxbrfu.supabase.co"
_FALLBACK_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImtnZXF0Z3V5cHNmcmhyeXhicmZ1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODAzNjk0NTIsImV4cCI6MjA5NTk0NTQ1Mn0.oIPcHvCu9PQKbd1VFXH5jP8Zkr85IzDKi6sEWWyHNFg"


def _base_url() -> str:
    return st.secrets.get("SUPABASE_URL", _FALLBACK_URL)


def _api_key() -> str:
    return st.secrets.get("SUPABASE_KEY", _FALLBACK_KEY)


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


# ── 사용자 ──────────────────────────────────────────────────────────────
def signup(username: str, password: str) -> dict:
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


def login(username: str, password: str) -> dict:
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


# ── 랭킹 ─────────────────────────────────────────────────────────────────
def get_item_ranking(top_n: int = 10):
    r = requests.get(
        _rest("query_logs"),
        headers=_headers(),
        params={"select": "item"},
        timeout=10,
    )
    rows = r.json() if r.ok else []
    counts = Counter(row["item"] for row in rows if row.get("item"))
    return counts.most_common(top_n)


def get_query_ranking(top_n: int = 10):
    r = requests.get(
        _rest("query_logs"),
        headers=_headers(),
        params={"select": "query_text"},
        timeout=10,
    )
    rows = r.json() if r.ok else []
    counts = Counter(row["query_text"] for row in rows if row.get("query_text"))
    return counts.most_common(top_n)


def get_total_stats():
    r1 = requests.get(_rest("query_logs"), headers=_headers(), params={"select": "id"}, timeout=10)
    r2 = requests.get(_rest("users"), headers=_headers(), params={"select": "id"}, timeout=10)
    total = len(r1.json()) if r1.ok else 0
    users = len(r2.json()) if r2.ok else 0
    return total, users


# ── 게시판 ───────────────────────────────────────────────────────────────
def get_posts():
    r = requests.get(
        _rest("posts"),
        headers=_headers(),
        params={"select": "*", "order": "created_at.desc"},
        timeout=10,
    )
    return r.json() if r.ok else []


def create_post(username: str, title: str, content: str):
    requests.post(
        _rest("posts"),
        headers=_headers("return=minimal"),
        json={"username": username, "title": title, "content": content},
        timeout=10,
    )


def delete_post(post_id: str, username: str):
    requests.delete(
        _rest("posts"),
        headers=_headers(),
        params={"id": f"eq.{post_id}", "username": f"eq.{username}"},
        timeout=10,
    )


def get_comments(post_id: str):
    r = requests.get(
        _rest("comments"),
        headers=_headers(),
        params={"select": "*", "post_id": f"eq.{post_id}", "order": "created_at.asc"},
        timeout=10,
    )
    return r.json() if r.ok else []


def create_comment(post_id: str, username: str, content: str):
    requests.post(
        _rest("comments"),
        headers=_headers("return=minimal"),
        json={"post_id": post_id, "username": username, "content": content},
        timeout=10,
    )


def delete_comment(comment_id: str, username: str):
    requests.delete(
        _rest("comments"),
        headers=_headers(),
        params={"id": f"eq.{comment_id}", "username": f"eq.{username}"},
        timeout=10,
    )
