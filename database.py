from supabase import create_client, Client
import hashlib
import streamlit as st
from collections import Counter

SUPABASE_URL = "https://kgeqtguypsfrhryxbrfu.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImtnZXF0Z3V5cHNmcmhyeHdicmZ1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODAzNjk0NTIsImV4cCI6MjA5NTk0NTQ1Mn0.oIPcHvCu9PQKbd1VFXH5jP8Zkr85IzDKi6sEWWyHNFg"

@st.cache_resource
def get_db() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def hash_pw(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()

# ── 사용자 ──────────────────────────────────────────────────────────────
def signup(username: str, password: str) -> dict:
    db = get_db()
    if db.table("users").select("id").eq("username", username).execute().data:
        return {"ok": False, "msg": "이미 사용 중인 아이디예요."}
    db.table("users").insert({"username": username, "password": hash_pw(password)}).execute()
    return {"ok": True}

def login(username: str, password: str) -> dict:
    db = get_db()
    result = db.table("users").select("*").eq("username", username).eq("password", hash_pw(password)).execute()
    if not result.data:
        return {"ok": False, "msg": "아이디 또는 비밀번호가 틀렸어요."}
    return {"ok": True, "user": result.data[0]}

# ── 질문 로그 ────────────────────────────────────────────────────────────
def log_query(username: str, query: str, item: str = None):
    try:
        get_db().table("query_logs").insert({
            "username": username,
            "query_text": query,
            "item": item
        }).execute()
    except Exception:
        pass

def get_history(username: str, limit: int = 30):
    return get_db().table("query_logs").select("*")\
        .eq("username", username)\
        .order("created_at", desc=True)\
        .limit(limit).execute().data

# ── 랭킹 ─────────────────────────────────────────────────────────────────
def get_item_ranking(top_n: int = 10):
    rows = get_db().table("query_logs").select("item").execute().data
    counts = Counter(r["item"] for r in rows if r.get("item"))
    return counts.most_common(top_n)

def get_query_ranking(top_n: int = 10):
    rows = get_db().table("query_logs").select("query_text").execute().data
    counts = Counter(r["query_text"] for r in rows if r.get("query_text"))
    return counts.most_common(top_n)

def get_total_stats():
    db = get_db()
    total = len(db.table("query_logs").select("id").execute().data)
    users = len(db.table("users").select("id").execute().data)
    return total, users

# ── 게시판 ───────────────────────────────────────────────────────────────
def get_posts():
    return get_db().table("posts").select("*").order("created_at", desc=True).execute().data

def create_post(username: str, title: str, content: str):
    get_db().table("posts").insert({
        "username": username, "title": title, "content": content
    }).execute()

def delete_post(post_id: str, username: str):
    get_db().table("posts").delete().eq("id", post_id).eq("username", username).execute()

def get_comments(post_id: str):
    return get_db().table("comments").select("*").eq("post_id", post_id)\
        .order("created_at").execute().data

def create_comment(post_id: str, username: str, content: str):
    get_db().table("comments").insert({
        "post_id": post_id, "username": username, "content": content
    }).execute()

def delete_comment(comment_id: str, username: str):
    get_db().table("comments").delete().eq("id", comment_id).eq("username", username).execute()
