"""UI layer for market news tab."""

import math

import streamlit as st

from backend.services.news_service import get_news_sentiment_styles, get_news_service


NEWS_SOURCE_OPTIONS = ["vnexpress", "cafef", "cafebiz", "vietstock", "vnEconomy"]
NEWS_SOURCE_LABELS = {
    "vnexpress": "VnExpress",
    "cafef": "CafeF",
    "cafebiz": "CafeBiz",
    "vietstock": "VietStock",
    "vnEconomy": "VnEconomy",
}


def render_pagination_controls(total_pages):
    """Hiển thị điều hướng trang ở cuối tab."""
    st.divider()
    spacer_left, control_col, spacer_right = st.columns([1, 2, 1])

    with control_col:
        prev_col, info_col, next_col = st.columns([1, 1, 1], gap="small")

        prev_disabled = st.session_state.news_current_page <= 1
        next_disabled = st.session_state.news_current_page >= total_pages

        if prev_col.button("⬅️", width='stretch', disabled=prev_disabled, key="news_prev_btn"):
            st.session_state.news_current_page -= 1
            st.rerun()

        info_col.markdown(
            f"<div style='text-align:center; font-size:16px; font-weight:600;'>Trang {st.session_state.news_current_page} / {total_pages}</div>",
            unsafe_allow_html=True,
        )

        if next_col.button("➡️", width='stretch', disabled=next_disabled, key="news_next_btn"):
            st.session_state.news_current_page += 1
            st.rerun()


@st.cache_data(ttl=300, show_spinner=False)
def fetch_news_cached(source: str, max_articles: int = 50):
    """Cached service wrapper for news retrieval."""
    return get_news_service().fetch_news(source=source, max_articles=max_articles)


def render(ticker: str = None):
    """Hiển thị tab tin tức từ nhiều nguồn."""
    st.header("📰 Tin tức Thị trường Chứng khoán Việt Nam")

    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown(
            """
        <p style='color:#94a3b8'>
        Tin tức mới nhất về thị trường chứng khoán Việt Nam từ nhiều nguồn tin uy tín.
        </p>
        """,
            unsafe_allow_html=True,
        )

    with col2:
        news_source = st.selectbox(
            "📡 Chọn nguồn:",
            NEWS_SOURCE_OPTIONS,
            format_func=lambda x: NEWS_SOURCE_LABELS.get(x, x),
        )

    if "news_current_page" not in st.session_state:
        st.session_state.news_current_page = 1

    per_page = 5

    with st.spinner(f"🔍 Đang tải tin tức từ {news_source.upper()}..."):
        news, warning_message, error_message = fetch_news_cached(news_source, max_articles=50)

    if error_message:
        st.error(f"❌ {error_message}")
        return

    if warning_message:
        st.warning(warning_message)

    if not news:
        st.error(f"❌ Không thể tải tin tức từ nguồn {news_source.upper()}")
        st.markdown(
            """
        ### 🔧 Nguyên nhân có thể:

        1. **🌐 Kết nối mạng**: Kiểm tra internet của bạn
        2. **🚫 Website chặn**: Nguồn tin có thể chặn request tự động
        3. **🔒 Firewall/Antivirus**: Có thể đang chặn kết nối
        4. **⏱️ Timeout**: Server phản hồi quá chậm

        ### 💡 Giải pháp:

        - **Thử nguồn khác**: Chọn nguồn tin khác trong dropdown ở trên
        - Refresh lại trang sau vài giây
        - Kiểm tra kết nối internet
        """
        )
        return

    total_pages = max(1, math.ceil(len(news) / per_page))
    current_page = min(st.session_state.news_current_page, total_pages)
    if current_page != st.session_state.news_current_page:
        st.session_state.news_current_page = current_page
        st.rerun()

    start_idx = (current_page - 1) * per_page
    page_news = news[start_idx : start_idx + per_page]
    if not page_news and current_page > 1:
        st.session_state.news_current_page = 1
        st.rerun()

    for item in page_news:
        sentiment_styles = get_news_sentiment_styles(item["title"], item["content"])
        border_color = sentiment_styles["border"]
        background_style = sentiment_styles["background"]
        sentiment_label = sentiment_styles["label"]
        title_link = (
            f"<a href='{item['link']}' target='_blank' style='color:#0f172a; text-decoration:none;'>{item['title']}</a>"
        )

        with st.container():
            st.markdown(
                f"""
                <div style='
                    background: {background_style};
                    border-left: 4px solid {border_color};
                    padding: 15px;
                    border-radius: 8px;
                    margin-bottom: 15px;
                '>
                    <div style='display: flex; justify-content: space-between; align-items: center; gap: 16px;'>
                        <h4 style='color: #0f172a; margin: 0 0 10px 0; flex: 1;'>📰 {title_link}</h4>
                        <span style='font-size:12px; font-weight:600; color:{border_color}; padding:4px 10px; border:1px solid {border_color}; border-radius:999px;'>
                            {sentiment_label}
                        </span>
                    </div>
                    <p style='color: #6b7280; font-size: 14px; margin: 0;'>
                        📅 <b>Đăng lúc:</b> {item['date']}
                    </p>
                </div>
                """,
                unsafe_allow_html=True,
            )
            st.write(item["content"])
            st.markdown("<br>", unsafe_allow_html=True)

    render_pagination_controls(total_pages)
