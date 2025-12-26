# components/tables.py
import streamlit as st
import pandas as pd

def render_pagination_controls(
    current_page,
    total_pages,
    page_offset_key="page_offset",
    rows_per_page=10
):
    """
    Render pagination controls (First, Prev, Next, Last)
    
    Args:
        current_page: Current page number
        total_pages: Total number of pages
        page_offset_key: Session state key for page offset
        rows_per_page: Number of rows per page
    """
    nav_cols = st.columns([1, 1, 2, 1, 1])
    
    with nav_cols[0]:
        if st.button("⏮ First", disabled=(current_page == 1), use_container_width=True, key=f"{page_offset_key}_first"):
            st.session_state[page_offset_key] = 0
            st.rerun()
    
    with nav_cols[1]:
        if st.button("◀ Prev", disabled=(current_page == 1), use_container_width=True, key=f"{page_offset_key}_prev"):
            st.session_state[page_offset_key] = max(0, st.session_state[page_offset_key] - rows_per_page)
            st.rerun()
    
    with nav_cols[3]:
        if st.button("Next ▶", disabled=(current_page >= total_pages), use_container_width=True, key=f"{page_offset_key}_next"):
            st.session_state[page_offset_key] = min(
                st.session_state[page_offset_key] + rows_per_page,
                (total_pages - 1) * rows_per_page
            )
            st.rerun()
    
    with nav_cols[4]:
        if st.button("Last ⏭", disabled=(current_page >= total_pages), use_container_width=True, key=f"{page_offset_key}_last"):
            st.session_state[page_offset_key] = (total_pages - 1) * rows_per_page
            st.rerun()


def render_table_metrics(
    total_rows,
    current_page,
    total_pages,
    pending_changes=None,
    download_data=None,
    download_filename="data.csv"
):
    """
    Render metrics row above table (Total Records, Page, Pending Changes, Download)
    
    Args:
        total_rows: Total number of filtered rows
        current_page: Current page number
        total_pages: Total number of pages
        pending_changes: Number of pending changes (optional)
        download_data: CSV data for download (optional)
        download_filename: Filename for download
    """
    if pending_changes is not None:
        col1, col2, col3, col4 = st.columns([2, 2, 2, 2])
    else:
        col1, col2, col3 = st.columns([2, 2, 2])
    
    with col1:
        st.metric("Filtered Records", f"{total_rows:,}")
    
    with col2:
        st.metric("Page", f"{current_page} / {total_pages}")
    
    if pending_changes is not None:
        with col3:
            if pending_changes > 0:
                st.metric("Pending Changes", f"{pending_changes}", delta="Unsaved")
            else:
                st.metric("Pending Changes", "0")
    
    # Download button
    download_col = col4 if pending_changes is not None else col3
    with download_col:
        if download_data is not None:
            st.download_button(
                label="📥 Download",
                data=download_data,
                file_name=download_filename,
                mime="text/csv",
                use_container_width=True
            )


def render_table_headers(columns, widths=None):
    """
    Render table headers
    
    Args:
        columns: List of column names
        widths: List of column widths (optional)
    """
    if widths is None:
        widths = [1.5] * len(columns)
    
    header_cols = st.columns(widths)
    
    for i, col in enumerate(columns):
        header_cols[i].markdown(f"<div class='header-text'>{col}</div>", unsafe_allow_html=True)