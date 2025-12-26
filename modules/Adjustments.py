# pages/2_📊_Adjustments.py
import streamlit as st
import pandas as pd
import time
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
from utils.data_loader import get_snowpark_session, get_current_user
from utils.validators import validate_adjustment_row, normalize_csv_data
from config.user_config import USER_CONFIG
from config.settings import PERIOD_OPTIONS, ADJ_TYPE_OPTIONS, MONTHS, YEARS, CUSTOM_CSS
from components.preview import (
    generate_changes_from_csv,
    render_preview_and_confirm
)
from components.filters import (
    render_multiselect_filters,
    render_clear_all_filters_button,
    apply_filters_to_dataframe
)
from components.adjustments_helper import (
            build_coa_lookup,
            prepare_upsert_dataframe,
            execute_adjustment_upsert_merge
        )
        

# ============================================
# PAGE CONFIG
# ============================================
st.set_page_config(
    page_title="Adjustments",
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
    st.session_state.selected_company = 'Access Holdings'

company = st.session_state.selected_company
config = USER_CONFIG.get(company, {})

# Extract config
ADJUSTMENTS_TABLE = config["adjustments_table"]
COA_TABLE = config["table_name"]
ADJ_PRIMARY_KEYS = config["adj_primary_keys"]
ADJ_DISPLAY_COLUMNS = config["adj_display_columns"]


# Full primary key
FULL_PRIMARY_KEYS = ADJ_PRIMARY_KEYS + ['PERIOD', 'ADJ_TYPE']
CSV_KEYS = ADJ_PRIMARY_KEYS + ['MONTH','YEAR', 'ADJ_TYPE']
# ============================================
# LOAD DATA
# ============================================
@st.cache_data(show_spinner="Loading COA data...", ttl=600)
def load_coa_data():
    """Load COA reference data"""
    all_cols = ADJ_PRIMARY_KEYS + ADJ_DISPLAY_COLUMNS
    unique_cols = list(dict.fromkeys(all_cols))
    
    df = session.table(COA_TABLE).select(unique_cols).to_pandas()
    
    for pk in ADJ_PRIMARY_KEYS:
        df[pk] = pd.to_numeric(df[pk], errors='coerce').fillna(0).astype(int)
    
    for col in ADJ_DISPLAY_COLUMNS:
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str).str.strip()
    
    return df

@st.cache_data(show_spinner="Loading adjustments...", ttl=600)
def load_adjustments_data():
    """Load adjustments data"""
    all_cols = (ADJ_PRIMARY_KEYS + ADJ_DISPLAY_COLUMNS + 
                ['PERIOD', 'ADJ_TYPE', 'AMOUNT'])
    unique_cols = list(dict.fromkeys(all_cols))
    
    df = session.table(ADJUSTMENTS_TABLE).select(unique_cols).to_pandas()
    
    for pk in ADJ_PRIMARY_KEYS:
        df[pk] = pd.to_numeric(df[pk], errors='coerce').fillna(0).astype(int)
    
    df['AMOUNT'] = pd.to_numeric(df['AMOUNT'], errors='coerce').fillna(0)
    
    for col in ADJ_DISPLAY_COLUMNS + ['PERIOD', 'ADJ_TYPE']:
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str).str.strip()
    
    return df

coa_df = load_coa_data()
adjustments_df = load_adjustments_data()

# ============================================
# HELPER FUNCTIONS
# ============================================
def create_csv_template():
    headers = CSV_KEYS + ['AMOUNT']
    return pd.DataFrame(columns=headers).to_csv(index=False)

def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

def cleanup_csv_upload():
    st.session_state.changes_to_apply = []
    st.session_state.show_preview = False

# ============================================
# SESSION STATE INIT
# ============================================
if 'show_preview' not in st.session_state:
    st.session_state.show_preview = False

if 'changes_to_apply' not in st.session_state:
    st.session_state.changes_to_apply = []

if 'pending_amount_changes' not in st.session_state:
    st.session_state.pending_amount_changes = {}

if 'adj_active_tab' not in st.session_state or st.session_state.adj_active_tab not in ["🖊️ Add/Update & Bulk Edit", "📤 CSV Upload", "📖 COA Reference"]:
    st.session_state.adj_active_tab = "🖊️ Add/Update & Bulk Edit"

# ============================================
# HEADER
# ============================================
st.title(f"📊 Adjustments - {company}")
st.caption(f"Table: **{ADJUSTMENTS_TABLE}**")

tab_options = ["🖊️ Add/Update & Bulk Edit", "📤 CSV Upload", "📖 COA Reference"]

# Get index safely
try:
    current_index = tab_options.index(st.session_state.get('adj_active_tab', tab_options[0]))
except ValueError:
    current_index = 0
    st.session_state.adj_active_tab = tab_options[0]

selected_tab = st.radio(
    "Select Action:",
    tab_options,
    horizontal=True,
    key="adj_tab_selector",
    index=current_index
)
# Update session state
st.session_state.adj_active_tab = selected_tab

st.divider()

# ========================================
# TAB 1: ADD/UPDATE + BULK EDIT
# ========================================
if selected_tab == "🖊️ Add/Update & Bulk Edit":
    # ============================================
    # SECTION 1: ADD/UPDATE SINGLE RECORD
    # ============================================
    st.subheader("➕ Add or Update Single Record")
    
    # Create a container for better alignment
    with st.container():
        # Determine number of columns needed
        num_cols = 1 + len(ADJ_DISPLAY_COLUMNS) + 2
        
        # Create columns
        input_cols = st.columns(num_cols)
        col_idx = 0
        
        # ADJ_TYPE selection
        selected_adj_type = input_cols[col_idx].selectbox(
            "ADJ_TYPE",
            ADJ_TYPE_OPTIONS,
            key="adj_input_type"
        )
        col_idx += 1
        
        force_unknown = selected_adj_type in ('Lender Adjustment', 'Pro-Forma Adjustment')
        
        # Collect selections
        selected_pks = {}
        selected_displays = {}
        
        # Reorder display columns to put ACCOUNT_NUMBER first
        ordered_display_cols = []
        if 'ACCOUNT_NUMBER' in ADJ_DISPLAY_COLUMNS:
            ordered_display_cols.append('ACCOUNT_NUMBER')
        for col in ADJ_DISPLAY_COLUMNS:
            if col != 'ACCOUNT_NUMBER':
                ordered_display_cols.append(col)
        
        # Loop through display columns in order
        for display_col in ordered_display_cols:
            if display_col not in coa_df.columns:
                st.error(f"Column '{display_col}' not found in COA data")
                continue
            
            # Find corresponding primary key
            try:
                pk_idx = ADJ_DISPLAY_COLUMNS.index(display_col)
                if pk_idx < len(ADJ_PRIMARY_KEYS):
                    pk = ADJ_PRIMARY_KEYS[pk_idx]
                else:
                    pk = None
            except ValueError:
                pk = None
            
            if force_unknown:
                selected_displays[display_col] = 'Unknown'
                if pk:
                    selected_pks[pk] = 0
                
                input_cols[col_idx].selectbox(
                    display_col,
                    ['Unknown'],
                    index=0,
                    key=f"adj_input_{display_col}",
                    disabled=True
                )
            else:
                # Apply cascading filter
                filtered_coa = coa_df.copy()
                
                # Cascade from ACCOUNT_NUMBER
                if display_col != 'ACCOUNT_NUMBER' and 'ACCOUNT_NUMBER' in selected_displays:
                    filtered_coa = filtered_coa[filtered_coa['ACCOUNT_NUMBER'] == selected_displays['ACCOUNT_NUMBER']]
                
                # Cascade from previously selected display columns
                for prev_col in ordered_display_cols:
                    if prev_col == display_col:
                        break
                    if prev_col in selected_displays and prev_col != 'ACCOUNT_NUMBER':
                        filtered_coa = filtered_coa[filtered_coa[prev_col] == selected_displays[prev_col]]
                
                unique_values = sorted(filtered_coa[display_col].dropna().unique().tolist())
                
                if not unique_values:
                    st.error(f"No values found for {display_col}")
                    st.stop()
                
                selected_display = input_cols[col_idx].selectbox(
                    display_col,
                    unique_values,
                    key=f"adj_input_{display_col}"
                )
                
                selected_displays[display_col] = selected_display
                
                # Get corresponding primary key value
                if pk:
                    filtered = filtered_coa[filtered_coa[display_col] == selected_display]
                    if len(filtered) > 0:
                        selected_pks[pk] = filtered[pk].iloc[0]
            
            col_idx += 1
        
        # PERIOD
        selected_period = input_cols[col_idx].selectbox(
            "PERIOD",
            PERIOD_OPTIONS,
            key="adj_input_period"
        )
        col_idx += 1
        
        # AMOUNT
        amount = input_cols[col_idx].number_input(
            "AMOUNT",
            value=0.0,
            format="%.2f",
            key="adj_input_amount"
        )
    
    # Info message if forcing unknown
    if force_unknown:
        st.info(f"ℹ️ {selected_adj_type} - All COA fields set to 'Unknown' (ID=0)")
    
    # Save button
    if st.button("💾 Save Record", type="primary", key="save_single_record"):
        try:
            where_conditions = [f"{pk} = '{selected_pks[pk]}'" for pk in ADJ_PRIMARY_KEYS]
            where_conditions.append(f"PERIOD = '{selected_period}'")
            where_conditions.append(f"ADJ_TYPE = '{selected_adj_type}'")
            
            check_query = f"""
                SELECT COUNT(*) as cnt FROM {ADJUSTMENTS_TABLE}
                WHERE {' AND '.join(where_conditions)}
            """
            exists = session.sql(check_query).collect()[0]['CNT'] > 0
            
            curr_time = pd.Timestamp.now()
            
            if exists:
                # UPDATE
                update_query = f"""
                    UPDATE {ADJUSTMENTS_TABLE}
                    SET AMOUNT = {amount},
                        LAST_UPDATED_BY = '{current_user}',
                        LAST_UPDATED_AT = '{curr_time}'
                    WHERE {' AND '.join(where_conditions)}
                """
                session.sql(update_query).collect()
                st.success("✅ Record updated successfully!")
            else:
                # INSERT
                cols = list(ADJ_PRIMARY_KEYS) + list(ADJ_DISPLAY_COLUMNS)
                vals = [f"'{selected_pks[pk]}'" for pk in ADJ_PRIMARY_KEYS] + [f"'{selected_displays[dis]}'" for dis in ADJ_DISPLAY_COLUMNS]
                
                cols.extend(['PERIOD', 'ADJ_TYPE', 'AMOUNT', 'CREATED_BY', 'CREATED_AT'])
                vals.extend([f"'{selected_period}'", f"'{selected_adj_type}'", str(amount), f"'{current_user}'", f"'{curr_time}'"])
                
                insert_query = f"""
                    INSERT INTO {ADJUSTMENTS_TABLE} ({', '.join(cols)})
                    VALUES ({', '.join(vals)})
                """
                session.sql(insert_query).collect()
                st.success("✅ Record inserted successfully!")
            
            st.cache_data.clear()
            time.sleep(0.5)
            st.rerun()
            
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")
            st.exception(e)
    
    st.divider()
       
    # ============================================
    # SECTION 2: BULK EDIT
    # ============================================
    st.subheader("✏️ Bulk Edit Existing Records")
    
    # Filters
    st.markdown("##### 🔍 Filter Records")
    
    render_multiselect_filters(
        display_columns=ADJ_DISPLAY_COLUMNS,
        reference_df=coa_df,
        applied_filters_key='adj_applied_filters',
        temp_filters_key='adj_temp_filters',
        filter_version_key='adj_filter_version'
    )
    
    render_clear_all_filters_button(
        display_columns=ADJ_DISPLAY_COLUMNS,
        applied_filters_key='adj_applied_filters',
        temp_filters_key='adj_temp_filters',
        filter_version_key='adj_filter_version'
    )
    
    st.divider()
    
    # Apply filters
    filtered_adj_df = apply_filters_to_dataframe(
        adjustments_df,
        ADJ_DISPLAY_COLUMNS,
        st.session_state.get('adj_applied_filters', {})
    )
    
    # Prepare display
    display_cols = ADJ_DISPLAY_COLUMNS + ['PERIOD', 'ADJ_TYPE', 'AMOUNT']
    if len(filtered_adj_df) > 0:
        display_df = filtered_adj_df[display_cols].copy()
    else:
        display_df = pd.DataFrame(columns=display_cols)
    
    st.caption(f"Showing {len(display_df):,} of {len(adjustments_df):,} records")
    
    # Editable grid
    if len(display_df) > 0:
        gb = GridOptionsBuilder.from_dataframe(display_df)
        gb.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=50)
        gb.configure_side_bar()
        gb.configure_default_column(editable=False, filter=True, sortable=True, resizable=True)
        gb.configure_column('AMOUNT', editable=True, type=['numericColumn'])
        
        grid_response = AgGrid(
            display_df,
            gridOptions=gb.build(),
            update_mode=GridUpdateMode.VALUE_CHANGED,
            fit_columns_on_grid_load=True,
            enable_enterprise_modules=False,
            height=600,
            theme="streamlit",
            data_return_mode=DataReturnMode.FILTERED_AND_SORTED
        )
        
        # Track changes
        if grid_response and 'data' in grid_response:
            edited_data = grid_response['data']
            
            for grid_idx in range(min(len(edited_data), len(filtered_adj_df))):
                edited_row = edited_data.iloc[grid_idx]
                original_row = filtered_adj_df.iloc[grid_idx]
                
                # Build unique key
                key_parts = [str(original_row[pk]) for pk in ADJ_PRIMARY_KEYS]
                key_parts.append(str(original_row['PERIOD']))
                key_parts.append(str(original_row['ADJ_TYPE']))
                row_key = "|".join(key_parts)
                
                # Check if AMOUNT changed
                old_amount = float(original_row['AMOUNT'])
                new_amount = float(edited_row['AMOUNT'])
                
                if abs(new_amount - old_amount) > 0.01:
                    st.session_state.pending_amount_changes[row_key] = {
                        'primary_keys': {pk: original_row[pk] for pk in ADJ_PRIMARY_KEYS},
                        'period': original_row['PERIOD'],
                        'adj_type': original_row['ADJ_TYPE'],
                        'old_amount': old_amount,
                        'new_amount': new_amount
                    }
                elif row_key in st.session_state.pending_amount_changes:
                    del st.session_state.pending_amount_changes[row_key]
    else:
        st.info("No records match filters.")
    
    st.divider()
    # Preview button
    if st.button("🔍 Preview Changes", use_container_width=False, key="preview_bulk"):
        if not st.session_state.pending_amount_changes:
            st.info("ℹ️ No changes detected.")
        else:
            st.session_state.show_preview = True
    # Preview modal

    # # Preview modal
    if st.session_state.show_preview and st.session_state.pending_amount_changes:
       
        st.markdown("---")
        st.subheader("📋 Preview Changes")
        st.info(f"Reviewing {len(st.session_state.pending_amount_changes)} change(s)")
        
        # Build preview table
        preview_data = []
        for change in st.session_state.pending_amount_changes.values():
            preview_row = {}
            for pk in ADJ_PRIMARY_KEYS:
                preview_row[pk] = change['primary_keys'][pk]
            preview_row['PERIOD'] = change['period']
            preview_row['ADJ_TYPE'] = change['adj_type']
            preview_row['Old Amount'] = f"{change['old_amount']:.2f}"
            preview_row['New Amount'] = f"{change['new_amount']:.2f}"
            preview_row['Change'] = f"{change['new_amount'] - change['old_amount']:+.2f}"
            preview_data.append(preview_row)
        
        st.dataframe(pd.DataFrame(preview_data), use_container_width=True, hide_index=True)
        st.divider()
        
        # Action buttons
        col1, col2 = st.columns([1, 1])
        
        with col1:
            if st.button("✅ Save All Changes", type="primary", use_container_width=True, key="save_bulk"):
                with st.spinner("Updating records via MERGE..."):
                    # Build COA lookup
                    coa_lookup = build_coa_lookup(coa_df, ADJ_PRIMARY_KEYS, ADJ_DISPLAY_COLUMNS)
                    
                    # Prepare dataframe for MERGE
                    upsert_df = prepare_upsert_dataframe(
                        data_source=st.session_state.pending_amount_changes,
                        primary_keys=ADJ_PRIMARY_KEYS,
                        display_columns=ADJ_DISPLAY_COLUMNS,
                        coa_lookup=coa_lookup,
                        source_type='bulk_edit'
                    )
                    
                    # Execute MERGE (UPDATE only, no INSERT for bulk edit)
                    success, insert_count, update_count, error_msg = execute_adjustment_upsert_merge(
                        upsert_df=upsert_df,
                        table_name=ADJUSTMENTS_TABLE,
                        primary_keys=ADJ_PRIMARY_KEYS,
                        display_columns=ADJ_DISPLAY_COLUMNS,
                        session=session,
                        current_user=current_user,
                        upsert_mode=False  # UPDATE only
                    )
                
                if success:
                    st.success(f"✅ Successfully updated {update_count} record(s)!")
                    
                    # Clear changes and reload
                    st.session_state.pending_amount_changes = {}
                    st.session_state.show_preview = False
                    st.cache_data.clear()
                    time.sleep(2)
                    st.rerun()
                else:
                    st.error(f"❌ Update failed: {error_msg}")
        
        with col2:
            if st.button("🔙 Back to Edit", use_container_width=True, key="close_preview"):
                st.session_state.show_preview = False
                st.rerun()

# ========================================
# TAB 2: CSV UPLOAD
# ========================================
elif selected_tab == "📤 CSV Upload":
    from components.adjustments_helper import (
        normalize_adjustment_csv,
        validate_adjustment_csv,
        create_adjustment_csv_template,
        build_coa_lookup,
        prepare_upsert_dataframe,
        execute_adjustment_upsert_merge,
        convert_month_year_to_period
    )
    
    st.subheader("📤 Bulk Upload from CSV")
    
    # Download buttons
    col1, col2 = st.columns([3, 3])
    with col1:
        template_csv = create_adjustment_csv_template(ADJ_PRIMARY_KEYS)
        st.download_button(
            label="📥 Download Template",
            data=template_csv,
            file_name="adjustments_template.csv",
            mime="text/csv",
            use_container_width=True
        )
    with col2:
        st.download_button(
            label="📥 Download Full Data",
            data=convert_df_to_csv(adjustments_df),
            file_name=f"adjustments_full_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True
        )
    
    st.divider()
    
    # File uploader
    uploaded_file = st.file_uploader("📤 Upload CSV", type=["csv"], key="adj_bulk_upload")
    
    if uploaded_file:
        try:
            # Read CSV
            csv_df = pd.read_csv(uploaded_file, dtype=str)
            
            # Normalize data
            csv_df_normalized = normalize_adjustment_csv(csv_df, ADJ_PRIMARY_KEYS)
            
            # Check required columns
            required_cols = ADJ_PRIMARY_KEYS + ['MONTH', 'YEAR', 'ADJ_TYPE', 'AMOUNT']
            missing = [c for c in required_cols if c not in csv_df_normalized.columns]
            
            if missing:
                st.error(f"❌ Missing required columns: {missing}")
            else:
                st.success("✅ File uploaded successfully!")
                st.dataframe(csv_df_normalized.head(20))
                
                # Check for duplicates
                duplicate_check_cols = ADJ_PRIMARY_KEYS + ['MONTH', 'YEAR', 'ADJ_TYPE']
                duplicate_mask = csv_df_normalized.duplicated(subset=duplicate_check_cols, keep=False)
                
                if duplicate_mask.any():
                    duplicate_rows = csv_df_normalized[duplicate_mask]
                    st.error(f"❌ Found {len(duplicate_rows)} duplicate rows!")
                    st.dataframe(duplicate_rows[duplicate_check_cols + ['AMOUNT']])
                    st.stop()
                
                # Create months dict for validation
               
                years_list = [int(y) for y in YEARS]
                
                # Validate
                validation_errors, valid_rows = validate_adjustment_csv(
                    csv_df_normalized,
                    ADJ_PRIMARY_KEYS,
        
                    years_list,
                    ADJ_TYPE_OPTIONS
                )
                
                if validation_errors:
                    st.error("❌ Validation failed:")
                    for error in validation_errors:
                        st.markdown(error)
                else:
                    st.success(f"✅ All validations passed! {len(valid_rows)} rows ready.")
                    
                    # Apply button
                    if st.button("✅ Apply Bulk Upload", type="primary", key="apply_adj_bulk"):
                        with st.spinner("Processing bulk upload via MERGE..."):
                            # Get only valid rows
                            valid_csv_df = csv_df_normalized.iloc[valid_rows].reset_index(drop=True)
                            
                            # Build COA lookup
                            coa_lookup = build_coa_lookup(coa_df, ADJ_PRIMARY_KEYS, ADJ_DISPLAY_COLUMNS)
                            
                            # Prepare dataframe for MERGE
                            upsert_df = prepare_upsert_dataframe(
                                data_source=valid_csv_df,
                                primary_keys=ADJ_PRIMARY_KEYS,
                                display_columns=ADJ_DISPLAY_COLUMNS,
                                coa_lookup=coa_lookup,
                                source_type='csv',
                               
                            )
                            
                            # # Check for Unknown display values
                            # for display_col in ADJ_DISPLAY_COLUMNS:
                            #     unknown_mask = upsert_df[display_col] == 'Unknown'
                            #     if unknown_mask.any():
                            #         unknown_count = unknown_mask.sum()
                            #         st.warning(f"⚠️ {unknown_count} row(s) have Unknown {display_col}")
                            
                            # Execute MERGE (UPSERT mode: INSERT + UPDATE)
                            success, insert_count, update_count, error_msg = execute_adjustment_upsert_merge(
                                upsert_df=upsert_df,
                                table_name=ADJUSTMENTS_TABLE,
                                primary_keys=ADJ_PRIMARY_KEYS,
                                display_columns=ADJ_DISPLAY_COLUMNS,
                                session=session,
                                current_user=current_user,
                                upsert_mode=True  # UPSERT (INSERT + UPDATE)
                            )
                        
                        if success:
                            results = []
                            if insert_count > 0:
                                results.append(f"Inserted {insert_count} row(s)")
                            if update_count > 0:
                                results.append(f"Updated {update_count} row(s)")
                            
                            st.success(f"✅ Bulk upload completed! {' | '.join(results)}")
                        else:
                            st.error(f"❌ Upload failed: {error_msg}")
                        
                        # Clear state and reload
                        st.session_state.pending_amount_changes = {}
                        st.session_state.show_preview = False
                        st.cache_data.clear()
                        time.sleep(2)
                        st.rerun()
        
        except Exception as e:
            st.error(f"❌ Error reading file: {str(e)}")
            st.exception(e)
# ========================================
# TAB 3: COA REFERENCE
# ========================================
elif selected_tab == "📖 COA Reference":
    st.subheader("📖 COA Reference")
    st.caption(f"Data from {COA_TABLE}")
    
    MAX_ROWS = 35000
    display_df = coa_df.head(MAX_ROWS) if len(coa_df) > MAX_ROWS else coa_df
    
    if len(coa_df) > MAX_ROWS:
        st.warning(f"⚠️ Showing {MAX_ROWS:,} of {len(coa_df):,} rows")
    
    gb = GridOptionsBuilder.from_dataframe(display_df)
    gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=25)
    gb.configure_side_bar()
    gb.configure_default_column(editable=False, filter=True, sortable=True, resizable=True)
    
    AgGrid(
        display_df,
        gridOptions=gb.build(),
        update_mode=GridUpdateMode.MODEL_CHANGED,
        enable_enterprise_modules=False,
        height=600,
        theme="streamlit"
    )
    
    st.caption(f"Total: {len(display_df):,}")
    
    st.download_button(
        label="📥 Download",
        data=convert_df_to_csv(coa_df),
        file_name=f"coa_reference_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )