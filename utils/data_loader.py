# utils/data_loader.py
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
from config.settings import METRIC_COLUMNS, CASHFLOW_COLUMNS, ADDITIONAL_COLUMNS, MAPPING_COLS

# Get session once and cache it
@st.cache_resource
def get_snowpark_session():
    """Cache the Snowpark session"""
    return get_active_session()


def normalize(value):
    """Normalize values for comparison"""
    if pd.isna(value) or value is None or str(value).strip() == "":
        return ""
    return str(value).strip()


@st.cache_data(ttl=3600, show_spinner="Loading metric hierarchy...")
def load_metric_hierarchy(metric_hierarchy_table):
    """Load metric hierarchy table"""
    session = get_snowpark_session()
    query = f"SELECT * FROM {metric_hierarchy_table}"
    df = session.sql(query).to_pandas()
    
    for col in METRIC_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(normalize)
    
    return df


@st.cache_data(ttl=3600, show_spinner="Loading cashflow hierarchy...")
def load_cashflow_hierarchy(cashflow_hierarchy_table):
    """Load cashflow hierarchy table"""
    session = get_snowpark_session()
    query = f"SELECT * FROM {cashflow_hierarchy_table}"
    df = session.sql(query).to_pandas()
    
    for col in CASHFLOW_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(normalize)
    
    return df


@st.cache_data(ttl=1800, show_spinner="Loading COA data...")
def load_actual_data(table_name, keys, display_cols):
    """Load COA mapping table with display names"""
    session = get_snowpark_session()
    query = f"SELECT * FROM {table_name}"
    df = session.sql(query).to_pandas()
    df = df.convert_dtypes()
    
    # Cast PRIMARY_KEYS from float to int
    for pk in keys:
        if pk in df.columns:
            df[pk] = df[pk].apply(lambda x: int(x) if pd.notna(x) and x != '' else 0)
    
    # Normalize all columns
    for col in METRIC_COLUMNS + display_cols + CASHFLOW_COLUMNS + ADDITIONAL_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(normalize)
    
    cols = keys + display_cols + MAPPING_COLS
    df_trimmed = df[cols].drop_duplicates()
    
    return df_trimmed


@st.cache_data(ttl=600, show_spinner="Loading adjustments...")
def load_adjustments_data(adjustments_table):
    """Load adjustments table - SHORT TTL for frequent updates"""
    session = get_snowpark_session()
    query = f"SELECT * FROM {adjustments_table}"
    df = session.sql(query).to_pandas()
    df = df.convert_dtypes()
    return df


@st.cache_data(ttl=3600)
def load_debt_mapping():
    """Load debt mapping options"""
    session = get_snowpark_session()
    result = session.sql("SELECT DEBT_CATEGORY FROM DEBT_MAPPING").collect()
    return [row['DEBT_CATEGORY'] for row in result]


def get_current_user():
    """Get current Snowflake user"""
    session = get_snowpark_session()
    return session.sql("SELECT CURRENT_USER()").collect()[0][0]


def get_available_roles():
    """Get available roles for current user"""
    session = get_snowpark_session()
    return session.sql("SELECT CURRENT_AVAILABLE_ROLES()").collect()[0][0]