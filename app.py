import streamlit as st
import os
import re

# Streamlit Secrets → 환경변수로 등록
os.environ["AGRO_API_KEY"] = st.secrets.get("AGRO_API_KEY", "")
os.environ["GROQ_API_KEY"] = st.secrets.get("GROQ_API_KEY", "")

from chatbot import AgroChatBot
from database import log_query

st.set_page_config(page_title="밭봇", page_icon="🌾")
st.title("🌾 밭봇 — 전국 공영도매시장 실시간 경매정보")
st.caption("농가(생산자) 대상: 품목별 가격 조회 + 출하 타이밍 추천")

# ── 로그인 상태 표시 ─────────────────────────────────────────────────────
user = st.session_state.get("user")
if user:
    st.sidebar.success(f"👤 {user['username']} 님")
    if st.sidebar.button("로그아웃"):
        del st.session_state["user"]
        st.rerun()
else:
    st.sidebar.info("로그인하면 질문 히스토리가 저장돼요!")

# ── 품목 추출 (로깅용) ───────────────────────────────────────────────────
ITEMS = ["배추", "무", "고추", "대파", "양파", "감자", "딸기", "사과", "배",
         "당근", "마늘", "생강", "상추", "시금치", "호박", "오이", "토마토",
         "수박", "참외", "포도", "복숭아", "감귤", "귤",
         "옥수수", "소맥", "대두", "설탕", "커피"]

def extract_item(text: str):
    for item in ITEMS:
        if item in text:
            return item
    return None

# ── 챗봇 초기화 ─────────────────────────────────────────────────────────
if "bot" not in st.session_state:
    st.session_state.bot = AgroChatBot()

if "messages" not in st.session_state:
    st.session_state.messages = [
        {"role": "assistant", "content": (
            "안녕하세요! 밭봇입니다.\n\n"
            "이렇게 물어보세요:\n"
            "- 배추 가격 얼마예요?\n"
            "- 대파 지금 팔면 될까요?\n"
            "- 가락시장 사과 어제 시세\n"
            "- 이번달 제철 품목이 뭐예요?\n"
            "- 급등 품목 알려주세요\n\n"
            "지원 품목: 배추, 무, 고추, 대파, 양파, 감자, 딸기, 사과, 배"
        )}
    ]

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"].replace("\n", "  \n"))

if user_input := st.chat_input("질문을 입력하세요..."):
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.write(user_input)

    with st.chat_message("assistant"):
        with st.spinner("답변 중..."):
            response = st.session_state.bot.respond(user_input)
        st.markdown(response.replace("\n", "  \n"))  # 줄바꿈 보존

    st.session_state.messages.append({"role": "assistant", "content": response})

    # ── 질문 로깅 (로그인된 경우) ─────────────────────────────────────────
    if user:
        item = extract_item(user_input)
        log_query(user["username"], user_input, item)
