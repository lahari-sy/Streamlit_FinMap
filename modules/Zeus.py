# pages/3_📊_Zeus_Adjustments.py
import streamlit as st
import pandas as pd
import time
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from utils.data_loader import get_snowpark_session, get_current_user
from config.user_config import USER_CONFIG
from config.settings import ADJ_TYPE_OPTIONS, MONTHS, YEARS, CUSTOM_CSS
from utils.formatters import convert_df_to_csv
from components.adjustments_helper import (
    normalize_adjustment_csv,
    validate_adjustment_csv,
    convert_month_year_to_period
)
from components.preview import (
    render_preview_and_confirm,
    generate_changes_from_csv,
    execute_merge_operation
)

# ============================================
# PAGE CONFIG
# ============================================
st.set_page_config(
    page_title="Zeus Adjustments",
    page_icon="📊",
    layout="wide"
)

# ============================================
# APPLY GLOBAL CSS
# ============================================
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# ============================================
# SESSION & CONFIG
# ============================================
session = get_snowpark_session()
current_user = get_current_user()

if 'selected_company' not in st.session_state:
    st.session_state.selected_company = 'Zeus'

company = st.session_state.selected_company
config = USER_CONFIG.get(company, {})

# Extract config
ADJUSTMENTS_TABLE = config.get("adjustments_table", "ZEUS_ADJUSTMENTS")
METRIC_HIERARCHY_TABLE = config.get("metric_hierarchy_table", "ZEUS_METRIC_HIERARCHY")
ADJ_PRIMARY_KEYS = config.get("adj_primary_keys", ["SUBSIDIARY_ID"])
METRIC_COLUMNS = ["METRIC_L1", "METRIC_L2", "METRIC_L3", "METRIC_L4", "METRIC_L5", "METRIC_L6"]

# ============================================
# LOAD DATA
# ============================================
@st.cache_data(show_spinner="Loading adjustments...", ttl=600)
def load_adjustments_data():
    """Load adjustments table"""
    query = f"SELECT * FROM {ADJUSTMENTS_TABLE}"
    df = session.sql(query).to_pandas()
    df = df.convert_dtypes()
    return df


@st.cache_data(show_spinner="Loading metric hierarchy...", ttl=600)
def load_metric_hierarchy():
    """Load metric hierarchy reference"""
    df = session.table(METRIC_HIERARCHY_TABLE).to_pandas()
    
    # Ensure COA_ID is int
    if 'COA_ID' in df.columns:
        df['COA_ID'] = pd.to_numeric(df['COA_ID'], errors='coerce').fillna(0).astype(int)
    
    # Normalize metric columns
    for col in METRIC_COLUMNS:
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str).str.strip()
    
    return df

adj_df = load_adjustments_data()
metric_hierarchy_df = load_metric_hierarchy()

# ============================================
# SESSION STATE INIT
# ============================================
if 'zeus_active_tab' not in st.session_state or st.session_state.zeus_active_tab not in ["📤 CSV Upload", "📖 Metric Reference"]:
    st.session_state.zeus_active_tab = "📤 CSV Upload"

# ============================================
# HEADER
# ============================================
st.title(f"📊 Zeus Adjustments - {company}")
st.caption(f"Table: **{ADJUSTMENTS_TABLE}**")

# ============================================
# RADIO BUTTON TAB SELECTOR
# ============================================
tab_options = ["📤 CSV Upload", "📖 Metric Reference"]

try:
    current_index = tab_options.index(st.session_state.get('zeus_active_tab', tab_options[0]))
except ValueError:
    current_index = 0
    st.session_state.zeus_active_tab = tab_options[0]

selected_tab = st.radio(
    "Select Action:",
    tab_options,
    horizontal=True,
    key="zeus_tab_selector",
    index=current_index
)

# Update session state
st.session_state.zeus_active_tab = selected_tab

st.divider()

# ========================================
# TAB 1: CSV UPLOAD
# ========================================
if selected_tab == "📤 CSV Upload":
    st.subheader("📤 Bulk Upload from CSV")
    
    # Download buttons
    col1, col2 = st.columns([3, 3])
    with col1:
        # Template with METRIC columns
        template_headers = ADJ_PRIMARY_KEYS + METRIC_COLUMNS + ['MONTH', 'YEAR', 'ADJ_TYPE', 'AMOUNT']
        template_csv = pd.DataFrame(columns=template_headers).to_csv(index=False)
        
        st.download_button(
            label="📥 Download Template",
            data=template_csv,
            file_name="zeus_adjustments_template.csv",
            mime="text/csv",
            use_container_width=True
        )
    with col2:
        st.download_button(
            label="📥 Download Full Data",
            data=convert_df_to_csv(adj_df),
            file_name=f"zeus_adj_full_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True
        )
    
    st.divider()
    
    # File uploader
    uploaded_file = st.file_uploader("📤 Upload CSV", type=["csv"], key="zeus_adj_bulk_upload")
    
    if uploaded_file:
        try:
            # Read and normalize CSV
            csv_df = pd.read_csv(uploaded_file, dtype=str)
            csv_df_normalized = normalize_adjustment_csv(csv_df, ADJ_PRIMARY_KEYS)
            
            # Check required columns
            required_cols = ADJ_PRIMARY_KEYS + METRIC_COLUMNS + ['MONTH', 'YEAR', 'ADJ_TYPE', 'AMOUNT']
            missing = [c for c in required_cols if c not in csv_df_normalized.columns]
            
            if missing:
                st.error(f"❌ Missing required columns: {missing}")
            else:
                st.success("✅ File uploaded successfully!")
                st.dataframe(csv_df_normalized.head(20))
                
                # Convert MONTH/YEAR to PERIOD for duplicate check
                csv_df_normalized['PERIOD'] = csv_df_normalized.apply(
                    lambda row: convert_month_year_to_period(row['MONTH'], row['YEAR']),
                    axis=1
                )
                
                # Check for duplicates
                duplicate_check_cols = ADJ_PRIMARY_KEYS + METRIC_COLUMNS + ['PERIOD', 'ADJ_TYPE']
                duplicate_mask = csv_df_normalized.duplicated(subset=duplicate_check_cols, keep=False)
                
                if duplicate_mask.any():
                    duplicate_rows = csv_df_normalized[duplicate_mask]
                    st.error(f"❌ Found {len(duplicate_rows)} duplicate rows!")
                    st.dataframe(duplicate_rows[duplicate_check_cols + ['AMOUNT']])
                    st.stop()
                
                # Basic validation (MONTH, YEAR, ADJ_TYPE, AMOUNT)
                months_dict = {i+1: month for i, month in enumerate(MONTHS)}
                years_list = [int(y) for y in YEARS]
                
                validation_errors, valid_rows = validate_adjustment_csv(
                    csv_df_normalized,
                    ADJ_PRIMARY_KEYS,
                    years_list,
                    ADJ_TYPE_OPTIONS
                )
                
                # Additional: Validate METRIC hierarchy
                if not validation_errors:
                    placeholder = "NA"
                    
                    # Prepare metric hierarchy for validation
                    metric_hierarchy_clean = metric_hierarchy_df.copy()
                    metric_hierarchy_clean[METRIC_COLUMNS] = (
                        metric_hierarchy_clean[METRIC_COLUMNS]
                        .fillna(placeholder)
                        .replace("", placeholder)
                    )
                    
                    metric_hierarchy_clean['metric_concat'] = (
                        metric_hierarchy_clean['METRIC_L1'].astype(str) + '|' +
                        metric_hierarchy_clean['METRIC_L2'].astype(str) + '|' +
                        metric_hierarchy_clean['METRIC_L3'].astype(str) + '|' +
                        metric_hierarchy_clean['METRIC_L4'].astype(str) + '|' +
                        metric_hierarchy_clean['METRIC_L5'].astype(str) + '|' +
                        metric_hierarchy_clean['METRIC_L6'].astype(str)
                    )
                    
                    valid_metrics = set(metric_hierarchy_clean['metric_concat'])
                    
                    # Validate CSV metrics
                    csv_df_normalized[METRIC_COLUMNS] = (
                        csv_df_normalized[METRIC_COLUMNS]
                        .fillna(placeholder)
                        .replace("", placeholder)
                    )
                    
                    csv_df_normalized['metric_concat'] = (
                        csv_df_normalized['METRIC_L1'].astype(str) + '|' +
                        csv_df_normalized['METRIC_L2'].astype(str) + '|' +
                        csv_df_normalized['METRIC_L3'].astype(str) + '|' +
                        csv_df_normalized['METRIC_L4'].astype(str) + '|' +
                        csv_df_normalized['METRIC_L5'].astype(str) + '|' +
                        csv_df_normalized['METRIC_L6'].astype(str)
                    )
                    
                    # Validate each row's metric
                    invalid_metric_rows = []
                    for idx in valid_rows[:]:  # Copy list to modify during iteration
                        row = csv_df_normalized.iloc[idx]
                        if row['metric_concat'] not in valid_metrics:
                            pk_display = "-".join(str(row[pk]) for pk in ADJ_PRIMARY_KEYS)
                            validation_errors.append(
                                f"**Row {idx + 2}** ({pk_display}): Invalid Metric L1-L6 combination. Please refer to Metric Reference tab"
                            )
                            invalid_metric_rows.append(idx)
                    
                    # Remove invalid rows from valid_rows
                    valid_rows = [idx for idx in valid_rows if idx not in invalid_metric_rows]
                
                if validation_errors:
                    st.error("❌ Validation failed:")
                    for error in validation_errors:
                        st.markdown(error)
                else:
                    st.success(f"✅ All validations passed! {len(valid_rows)} rows ready.")
                    
                    # Apply button
                    if st.button("✅ Apply Bulk Upload", type="primary", key="apply_zeus_adj_bulk"):
                        with st.spinner("Processing bulk upload via MERGE..."):
                            # Get valid rows
                            valid_csv_df = csv_df_normalized.iloc[valid_rows].reset_index(drop=True)
                            
                            # Merge with metric_hierarchy to get COA_ID
                            valid_csv_df = valid_csv_df.merge(
                                metric_hierarchy_clean[["metric_concat", "COA_ID"]],
                                on="metric_concat",
                                how="left"
                            )
                            
                            # Drop temporary column
                            valid_csv_df = valid_csv_df.drop(columns=["metric_concat"])
                            
                            # Check for missing COA_IDs
                            missing_coa = valid_csv_df[valid_csv_df['COA_ID'].isna()]
                            if len(missing_coa) > 0:
                                st.error(f"❌ {len(missing_coa)} row(s) missing COA_ID after metric lookup!")
                                st.dataframe(missing_coa[ADJ_PRIMARY_KEYS + METRIC_COLUMNS])
                                st.stop()
                            
                            # Prepare full primary keys (includes COA_ID, PERIOD, ADJ_TYPE)
                            FULL_PRIMARY_KEYS = ADJ_PRIMARY_KEYS + ['COA_ID','PERIOD', 'ADJ_TYPE']
                          
                            # Normalize types for merge
                            for pk in ADJ_PRIMARY_KEYS :
                                valid_csv_df[pk] = pd.to_numeric(valid_csv_df[pk], errors='coerce').fillna(0).astype(int)
                            
                            valid_csv_df['PERIOD'] = valid_csv_df['PERIOD'].astype(str).str.strip()
                            valid_csv_df['ADJ_TYPE'] = valid_csv_df['ADJ_TYPE'].astype(str).str.strip()
                            valid_csv_df['AMOUNT'] = pd.to_numeric(valid_csv_df['AMOUNT'], errors='coerce').fillna(0)

                            common_cols = [col for col in FULL_PRIMARY_KEYS + ['AMOUNT'] if col in valid_csv_df.columns]
                            valid_csv_df = valid_csv_df[common_cols]
                            
                            # Also ensure adj_df has same order
                            adj_df = adj_df[common_cols]

                            # Generate changes for execution
                            changes, _ = generate_changes_from_csv(
                                new_df=valid_csv_df,
                                original_df=adj_df,
                                primary_keys=FULL_PRIMARY_KEYS,
                                mapping_cols=['AMOUNT']
                            )
                           
                            if not changes:
                                st.info("ℹ️ No changes detected (all values match existing data).")
                            else:
                                # Execute MERGE directly (UPSERT mode)
                                success, message, inserts, updates = execute_merge_operation(
                                    changes=changes,
                                    table_name=ADJUSTMENTS_TABLE,
                                    primary_keys=FULL_PRIMARY_KEYS,
                                    mapping_cols=['AMOUNT'],
                                    display_columns=None,  # No display columns for Zeus
                                    use_upsert=True  # INSERT + UPDATE
                                )
                                
                                if success:
                                    results = []
                                    if inserts > 0:
                                        results.append(f"Inserted {inserts} row(s)")
                                    if updates > 0:
                                        results.append(f"Updated {updates} row(s)")
                                    
                                    st.success(f"✅ Bulk upload completed! {' | '.join(results)}")
                                    
                                    # Clear state and reload
                                    st.cache_data.clear()
                                    time.sleep(2)
                                    st.rerun()
                                else:
                                    st.error(f"❌ Upload failed: {message}")
        
        except Exception as e:
            st.error(f"❌ Error reading file: {str(e)}")
            st.exception(e)


# ========================================
# TAB 2: METRIC REFERENCE (EDITABLE)
# ========================================
elif selected_tab == "📖 Metric Reference":
    from components.hierarchy_editor import render_hierarchy_editor, save_hierarchy_changes
    
    st.subheader("📖 Metric Hierarchy Reference")
    st.caption(f"Data from **{METRIC_HIERARCHY_TABLE}** table")
    
    # Load fresh data from DB
    db_df = session.table(METRIC_HIERARCHY_TABLE).to_pandas()
    original_df = db_df.copy()
    
    # Render editable grid
    edited_df, has_changes = render_hierarchy_editor(
        table_name=METRIC_HIERARCHY_TABLE,
        hierarchy_df=db_df,
        original_df=original_df,
        title="Metric Hierarchy Reference",
        caption=f"Edit and manage metric hierarchy data. Primary key: **COA_ID**",
        session_state_keys={
            'new_rows': 'new_rows_zeus_metric_hierarchy',
            'csv_processed': 'csv_processed_zeus_metric_hierarchy',
            'uploaded_data': 'uploaded_data_zeus_metric_hierarchy',
            'last_file_id': 'last_file_id_zeus_metric_hierarchy'
        },
        primary_key='COA_ID'  # Specify COA_ID as primary key
    )
    
    st.divider()
    
    # Save Changes button
    col1, col2 = st.columns([1, 5])
    
    with col1:
        if st.button("💾 Save Changes", type="primary", key="save_zeus_metric_hierarchy", disabled=not has_changes):
            # Get new row IDs
            new_row_ids = [
                row['COA_ID'] 
                for row in st.session_state.get('new_rows_zeus_metric_hierarchy', []) 
                if row.get('COA_ID')
            ]
            
            success, message,inserts,updates = save_hierarchy_changes(
                edited_df=edited_df,
                original_df=original_df,
                table_name=METRIC_HIERARCHY_TABLE,
                session=session,
                new_row_ids_list=new_row_ids,
                primary_key='COA_ID'  # Use COA_ID instead of ROW_ID
            )
            
            if success:
                if inserts > 0 or updates > 0:
                    st.success(f"✅ {message}")
                    
                    # Clear session state
                    st.session_state.new_rows_zeus_metric_hierarchy = []
                    st.session_state.csv_processed_zeus_metric_hierarchy = False
                    
                    if 'uploaded_data_zeus_metric_hierarchy' in st.session_state:
                        del st.session_state.uploaded_data_zeus_metric_hierarchy
                    if 'last_file_id_zeus_metric_hierarchy' in st.session_state:
                        del st.session_state.last_file_id_zeus_metric_hierarchy
                    
                    st.cache_data.clear()
                    time.sleep(2)
                    st.rerun()
                else:
                    st.info("ℹ️ No changes to save")
            else:
                st.error(f"❌ {message}")
    
    with col2:
        if has_changes:
            st.caption(f"⚠️ You have unsaved changes")
