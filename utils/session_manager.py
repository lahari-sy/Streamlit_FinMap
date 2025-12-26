# utils/session_manager.py
import streamlit as st
from config.user_config import USER_CONFIG

def initialize_session_state(company, config):
    """Initialize all session state variables"""
    
    # Page navigation
    if 'active_page' not in st.session_state:
        st.session_state.active_page = "COA"
    
    if 'active_tab' not in st.session_state:
        st.session_state.active_tab = 0
    
    # Company selection
    if 'selected_company' not in st.session_state:
        st.session_state.selected_company = company
    
    # COA page state
    if 'page_offset' not in st.session_state:
        st.session_state.page_offset = 0
    
    if 'show_preview' not in st.session_state:
        st.session_state.show_preview = False
    
    if 'changes_to_apply' not in st.session_state:
        st.session_state.changes_to_apply = None
    
    if 'edited_rows' not in st.session_state:
        st.session_state.edited_rows = {}
    
    # Filter state - COA (USE DISPLAY_COLUMNS)
    if 'filter_version' not in st.session_state:
        st.session_state.filter_version = 0
    
    if 'temp_filters' not in st.session_state:
        display_columns = config.get("display_columns", [])
        st.session_state.temp_filters = {col: [] for col in display_columns}
    
    if 'applied_filters' not in st.session_state:
        display_columns = config.get("display_columns", [])
        st.session_state.applied_filters = {col: [] for col in display_columns}
    
    if 'show_unmapped_only' not in st.session_state:
        st.session_state.show_unmapped_only = False
    
    # Adjustments page state
    if 'adj_page_offset' not in st.session_state:
        st.session_state.adj_page_offset = 0
    
    if 'adj_filter_version' not in st.session_state:
        st.session_state.adj_filter_version = 0
    
    if 'adj_active_tab' not in st.session_state:
        st.session_state.adj_active_tab = "📝 Update via UI"
    
    # Adjustments filters (USE ADJ_DISPLAY_COLUMNS)
    if 'adj_temp_filters' not in st.session_state:
        adj_display_columns = config.get("adj_display_columns", [])
        st.session_state.adj_temp_filters = {col: [] for col in adj_display_columns}
        st.session_state.adj_temp_filters['PERIOD'] = []
        st.session_state.adj_temp_filters['ADJ_TYPE'] = []
    
    if 'adj_applied_filters' not in st.session_state:
        adj_display_columns = config.get("adj_display_columns", [])
        st.session_state.adj_applied_filters = {col: [] for col in adj_display_columns}
        st.session_state.adj_applied_filters['PERIOD'] = []
        st.session_state.adj_applied_filters['ADJ_TYPE'] = []
    
    # Adjustments changes tracking
    if 'pending_adj_changes' not in st.session_state:
        st.session_state.pending_adj_changes = {}
    
    if 'adj_original_amounts' not in st.session_state:
        st.session_state.adj_original_amounts = {}
    
    if 'show_adj_preview' not in st.session_state:
        st.session_state.show_adj_preview = False
    
    if 'widget_reset_counter' not in st.session_state:
        st.session_state.widget_reset_counter = 0
    
    # Hierarchy management
    if "new_rows_hierarchy" not in st.session_state:
        st.session_state.new_rows_hierarchy = []
    
    if "csv_processed_hierarchy" not in st.session_state:
        st.session_state.csv_processed_hierarchy = False
    
    if 'file_uploader_key' not in st.session_state:
        st.session_state.file_uploader_key = 0


def reset_filters(filter_type="coa"):
    """Reset filters based on type"""
    if filter_type == "coa":
        config = USER_CONFIG.get(st.session_state.selected_company, {})
        display_columns = config.get("display_columns", [])
        
        # Reset applied filters
        st.session_state.applied_filters = {col: [] for col in display_columns}
        
        # Reset temp filters
        for col in display_columns:
            st.session_state[f'temp_{col}'] = []
        
        # Reset pagination and unmapped filter
        st.session_state.page_offset = 0
        st.session_state.show_unmapped_only = False
        st.session_state.filter_version += 1
    
    elif filter_type == "adj":
        config = USER_CONFIG.get(st.session_state.selected_company, {})
        adj_display_columns = config.get("adj_display_columns", [])
        
        st.session_state.adj_applied_filters = {col: [] for col in adj_display_columns}
        st.session_state.adj_applied_filters['PERIOD'] = []
        st.session_state.adj_applied_filters['ADJ_TYPE'] = []
        
        for col in adj_display_columns:
            st.session_state[f'adj_temp_{col}'] = []
        st.session_state['adj_temp_PERIOD'] = []
        st.session_state['adj_temp_ADJ_TYPE'] = []
        
        st.session_state.adj_page_offset = 0
        st.session_state.adj_filter_version += 1


def clear_company_state():
    """Clear state when company changes"""
    st.session_state.edited_rows = {}
    st.session_state.pending_adj_changes = {}
    st.session_state.adj_original_amounts = {}
    st.session_state.show_preview = False
    st.session_state.show_adj_preview = False
    st.session_state.page_offset = 0
    st.session_state.adj_page_offset = 0