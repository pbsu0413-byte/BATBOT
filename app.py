import streamlit as st
from chatbot import AgroChatBot

st.set_page_config(page_title="밭봇", page_icon="🌾")
st.title("🌾 밭봇 — 전국 공영도매시장 실시간 경매정보")
st.caption("농가(생산자) 대상: 품목별 가격 조회 + 출하 타이밍 추천")

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
        st.write(msg["content"])

if user_input := st.chat_input("질문을 입력하세요..."):
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.write(user_input)

    with st.chat_message("assistant"):
        with st.spinner("답변 중..."):
            response = st.session_state.bot.respond(user_input)
        st.write(response)

    st.session_state.messages.append({"role": "assistant", "content": response})
