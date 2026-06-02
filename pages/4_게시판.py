import streamlit as st
from database import (
    get_posts, create_post, delete_post,
    get_comments, create_comment, delete_comment
)

st.set_page_config(page_title="게시판 — 밭봇", page_icon="🌾")
st.title("📌 커뮤니티 게시판")

logged_in = bool(st.session_state.get("user"))
me = st.session_state["user"]["username"] if logged_in else None

# ── 글쓰기 ──────────────────────────────────────────────────────────────
if logged_in:
    with st.expander("✏️ 새 글 쓰기", expanded=False):
        with st.form("new_post_form", clear_on_submit=True):
            title = st.text_input("제목", placeholder="제목을 입력하세요")
            content = st.text_area("내용", placeholder="내용을 입력하세요", height=120)
            post_btn = st.form_submit_button("등록", type="primary")
        if post_btn:
            if title.strip() and content.strip():
                create_post(me, title.strip(), content.strip())
                st.success("게시글이 등록되었습니다!")
                st.rerun()
            else:
                st.error("제목과 내용을 모두 입력해주세요.")
else:
    st.info("✏️ 글을 쓰려면 **로그인**이 필요합니다.")

st.divider()

# ── 게시글 목록 ─────────────────────────────────────────────────────────
posts = get_posts()

if not posts:
    st.info("아직 게시글이 없어요. 첫 번째 글을 작성해보세요! 🌾")
else:
    for post in posts:
        with st.container(border=True):
            # 헤더: 제목 + 작성자 + 날짜
            col_title, col_info = st.columns([4, 1])
            with col_title:
                st.markdown(f"### {post['title']}")
            with col_info:
                date_str = str(post.get("created_at", ""))[:10]
                st.caption(f"**{post['username']}** · {date_str}")

            st.write(post["content"])

            # 삭제 버튼 (본인만)
            if logged_in and post["username"] == me:
                if st.button("🗑️ 삭제", key=f"del_{post['id']}"):
                    delete_post(post["id"], me)
                    st.rerun()

            # ── 댓글 ────────────────────────────────────────────────────
            with st.expander(f"💬 댓글 보기 / 달기", expanded=False):
                comments = get_comments(post["id"])

                if comments:
                    for c in comments:
                        c_col1, c_col2 = st.columns([5, 1])
                        with c_col1:
                            st.markdown(f"**{c['username']}** : {c['content']}")
                        with c_col2:
                            c_date = str(c.get("created_at", ""))[:10]
                            st.caption(c_date)
                            # 본인 댓글 삭제
                            if logged_in and c["username"] == me:
                                if st.button("삭제", key=f"dc_{c['id']}"):
                                    delete_comment(c["id"], me)
                                    st.rerun()
                else:
                    st.caption("아직 댓글이 없어요.")

                # 댓글 작성
                if logged_in:
                    with st.form(f"comment_form_{post['id']}", clear_on_submit=True):
                        new_comment = st.text_input("댓글 입력", placeholder="댓글을 입력하세요...", label_visibility="collapsed")
                        c_btn = st.form_submit_button("등록")
                    if c_btn and new_comment.strip():
                        create_comment(post["id"], me, new_comment.strip())
                        st.rerun()
