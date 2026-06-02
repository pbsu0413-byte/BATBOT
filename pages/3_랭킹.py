import streamlit as st
from database import get_item_ranking, get_query_ranking, get_total_stats

st.set_page_config(page_title="랭킹 — 밭봇", page_icon="🌾")
st.title("🏆 인기 랭킹")

# 전체 통계
total_queries, total_users = get_total_stats()
col1, col2 = st.columns(2)
col1.metric("총 질문 수", f"{total_queries:,}건")
col2.metric("총 사용자 수", f"{total_users:,}명")

st.divider()

tab1, tab2 = st.tabs(["🥬 인기 품목 TOP 10", "💬 인기 질문 TOP 10"])

MEDALS = ["🥇", "🥈", "🥉"]

with tab1:
    st.subheader("가장 많이 조회된 농산물")
    ranking = get_item_ranking(top_n=10)
    if not ranking:
        st.info("아직 데이터가 없어요.")
    else:
        for i, (item, count) in enumerate(ranking):
            medal = MEDALS[i] if i < 3 else f"{i+1}."
            with st.container(border=True):
                col_a, col_b = st.columns([5, 1])
                col_a.markdown(f"{medal} &nbsp; **{item}**")
                col_b.markdown(f"**{count}회**")

with tab2:
    st.subheader("가장 많이 들어온 질문")
    ranking2 = get_query_ranking(top_n=10)
    if not ranking2:
        st.info("아직 데이터가 없어요.")
    else:
        for i, (query, count) in enumerate(ranking2):
            medal = MEDALS[i] if i < 3 else f"{i+1}."
            with st.container(border=True):
                col_a, col_b = st.columns([5, 1])
                col_a.markdown(f"{medal} &nbsp; {query}")
                col_b.markdown(f"**{count}회**")
