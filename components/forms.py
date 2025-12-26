# components/forms.py
import streamlit as st
import pandas as pd


def get_current_values(row, primary_keys, mapping_cols):
    """Get current values for a row - either from edited_rows or original data"""
    from utils.formatters import get_edit_key
    
    edit_key = get_edit_key({pk: row[pk] for pk in primary_keys}, primary_keys)
    if edit_key in st.session_state.edited_rows:
        return st.session_state.edited_rows[edit_key]
    return {col: row[col] for col in mapping_cols}


def save_edit(primary_keys_dict, metric_values, primary_keys):
    """Save an edit to session state"""
    from utils.formatters import get_edit_key
    
    edit_key = get_edit_key(primary_keys_dict, primary_keys)
    st.session_state.edited_rows[edit_key] = metric_values.copy()
# def get_cascading_options(metric_hierarchy_df, level, parent_value=None, parent_level=None):
#     """Get cascading dropdown options for metrics"""
#     from utils.data_loader import normalize
    
#     if level == 1:
#         options = sorted(metric_hierarchy_df['METRIC_L1'].dropna().unique().tolist())
#         result = [opt for opt in options if opt != ""]
#         if "" in metric_hierarchy_df['METRIC_L1'].values or metric_hierarchy_df['METRIC_L1'].isna().any():
#             result.insert(0, "")
#         return result
    
#     if parent_value is None or normalize(parent_value) == "":
#         parent_col = f'METRIC_L{parent_level}'
#         current_col = f'METRIC_L{level}'
#         filtered = metric_hierarchy_df[metric_hierarchy_df[parent_col] == ""]
#         options = sorted(filtered[current_col].dropna().unique().tolist())
#         result = [opt for opt in options if opt != ""]
#         if "" in filtered[current_col].values or filtered[current_col].isna().any():
#             result.insert(0, "")
#         return result
    
#     parent_col = f'METRIC_L{parent_level}'
#     current_col = f'METRIC_L{level}'
#     filtered = metric_hierarchy_df[metric_hierarchy_df[parent_col] == parent_value]
#     options = sorted(filtered[current_col].dropna().unique().tolist())
#     result = [opt for opt in options if opt != ""]
#     if "" in filtered[current_col].values or filtered[current_col].isna().any():
#         result.insert(0, "")
#     return result


# def get_cashflow_cascading_options(cashflow_hierarchy_df, level, parent_value=None, parent_level=None):
#     """Get cascading dropdown options for cashflow"""
#     from utils.data_loader import normalize
    
#     if level == 1:
#         options = sorted(cashflow_hierarchy_df['CASHFLOW_L1'].dropna().unique().tolist())
#         result = [opt for opt in options if opt != ""]
#         if "" in cashflow_hierarchy_df['CASHFLOW_L1'].values or cashflow_hierarchy_df['CASHFLOW_L1'].isna().any():
#             result.insert(0, "")
#         return result
    
#     if parent_value is None or normalize(parent_value) == "":
#         parent_col = f'CASHFLOW_L{parent_level}'
#         current_col = f'CASHFLOW_L{level}'
#         filtered = cashflow_hierarchy_df[cashflow_hierarchy_df[parent_col] == ""]
#         options = sorted(filtered[current_col].dropna().unique().tolist())
#         result = [opt for opt in options if opt != ""]
#         if "" in filtered[current_col].values or filtered[current_col].isna().any():
#             result.insert(0, "")
#         return result
    
#     parent_col = f'CASHFLOW_L{parent_level}'
#     current_col = f'CASHFLOW_L{level}'
#     filtered = cashflow_hierarchy_df[cashflow_hierarchy_df[parent_col] == parent_value]
#     options = sorted(filtered[current_col].dropna().unique().tolist())
#     result = [opt for opt in options if opt != ""]
#     if "" in filtered[current_col].values or filtered[current_col].isna().any():
#         result.insert(0, "")
#     return result


# def get_current_values(row, primary_keys, mapping_cols):
#     """Get current values for a row - either from edited_rows or original data"""
#     from utils.formatters import get_edit_key
    
#     edit_key = get_edit_key({pk: row[pk] for pk in primary_keys}, primary_keys)
#     if edit_key in st.session_state.edited_rows:
#         return st.session_state.edited_rows[edit_key]
#     return {col: row[col] for col in mapping_cols}


# def save_edit(primary_keys_dict, metric_values, primary_keys):
#     """Save an edit to session state"""
#     from utils.formatters import get_edit_key
    
#     edit_key = get_edit_key(primary_keys_dict, primary_keys)
#     st.session_state.edited_rows[edit_key] = metric_values.copy()