import streamlit as st
from database import signup, login

st.set_page_config(page_title="로그인 — 밭봇", page_icon="🌾")
st.title("🔐 로그인 / 회원가입")

# 이미 로그인된 경우
if st.session_state.get("user"):
    user = st.session_state["user"]
    st.success(f"✅ **{user['username']}** 님으로 로그인 중입니다.")
    if st.button("로그아웃", type="primary"):
        del st.session_state["user"]
        st.rerun()
    st.stop()

tab1, tab2 = st.tabs(["🔑 로그인", "📝 회원가입"])

with tab1:
    with st.form("login_form"):
        username = st.text_input("아이디", placeholder="아이디 입력")
        password = st.text_input("비밀번호", type="password", placeholder="비밀번호 입력")
        submitted = st.form_submit_button("로그인", use_container_width=True, type="primary")

    if submitted:
        if not username or not password:
            st.error("아이디와 비밀번호를 모두 입력해주세요.")
        else:
            result = login(username, password)
            if result["ok"]:
                st.session_state["user"] = result["user"]
                st.success(f"환영합니다, {username} 님! 🎉")
                st.rerun()
            else:
                st.error(result["msg"])

with tab2:
    with st.form("signup_form"):
        new_username = st.text_input("아이디", placeholder="영문/숫자 조합 권장", key="su_id")
        new_password = st.text_input("비밀번호", type="password", placeholder="6자 이상 권장", key="su_pw")
        new_password2 = st.text_input("비밀번호 확인", type="password", placeholder="비밀번호 재입력", key="su_pw2")
        submitted2 = st.form_submit_button("회원가입", use_container_width=True)

    if submitted2:
        if not new_username or not new_password:
            st.error("아이디와 비밀번호를 모두 입력해주세요.")
        elif new_password != new_password2:
            st.error("비밀번호가 일치하지 않아요.")
        elif len(new_password) < 4:
            st.error("비밀번호는 4자 이상이어야 해요.")
        else:
            result = signup(new_username, new_password)
            if result["ok"]:
                st.success("가입 완료! 로그인 탭에서 로그인해주세요. ✅")
            else:
                st.error(result["msg"])
