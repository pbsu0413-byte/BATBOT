import streamlit as st
from database import get_history

st.set_page_config(page_title="히스토리 — 밭봇", page_icon="🌾")
st.title("📋 내 질문 히스토리")

if not st.session_state.get("user"):
    st.warning("🔐 로그인이 필요합니다. 왼쪽 사이드바에서 **로그인** 페이지로 이동해주세요.")
    st.stop()

user = st.session_state["user"]
st.caption(f"**{user['username']}** 님의 최근 30개 질문 기록")

rows = get_history(user["username"], limit=30)

if not rows:
    st.info("아직 질문 기록이 없어요. 밭봇에게 질문해보세요! 🌾")
else:
    for i, row in enumerate(rows, 1):
        with st.container(border=True):
            col1, col2 = st.columns([4, 1])
            with col1:
                st.write(f"**{i}.** {row['query_text']}")
                if row.get("item"):
                    st.caption(f"🏷️ 품목: {row['item']}")
            with col2:
                created = row.get("created_at", "")
                if created:
                    # 날짜만 표시
                    date_str = str(created)[:16].replace("T", " ")
                    st.caption(date_str)
