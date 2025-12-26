# pages/1_📊_COA_Mapping.py
import streamlit as st
import pandas as pd
import time
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import (
    CUSTOM_CSS, METRIC_COLUMNS, CASHFLOW_COLUMNS, MAPPING_COLS,
    ADDITIONAL_COLUMNS, ROWS_PER_PAGE
)
from config.user_config import USER_CONFIG
from utils.data_loader import (
    get_snowpark_session, load_metric_hierarchy, load_cashflow_hierarchy,
    load_actual_data, load_debt_mapping, get_current_user, normalize
)
from utils.formatters import (
    format_value_display, get_row_key, convert_df_to_csv,
    create_csv_template, get_edit_key
)
from utils.validators import validate_csv_hierarchy
from utils.cascade_helper import (
    load_metric_cascade,
    load_cashflow_cascade,
    get_options_from_tree,
    invalidate_cascade_cache,
    get_cascade_stats
)
from components.filters import (
    render_multiselect_filters,
    render_clear_all_filters_button,
    apply_filters_to_dataframe
)
from components.tables import render_pagination_controls, render_table_metrics, render_table_headers
from components.forms import get_current_values, save_edit
from components.preview import (generate_changes_from_edited_rows,render_preview_and_confirm,generate_changes_from_csv)
from components.hierarchy_editor import render_hierarchy_editor, save_hierarchy_changes



# Apply custom CSS
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# Get session and current user
session = get_snowpark_session()
current_user = get_current_user()

# Get company and config
company = st.session_state.get('selected_company', 'Access Holdings')
config = USER_CONFIG.get(company, {})

# Extract config values
TABLE_NAME = config["table_name"]
METRIC_HIERARCHY_TABLE = config["metric_hierarchy_table"]
PRIMARY_KEYS = config["primary_keys"]
DISPLAY_COLUMNS = config["display_columns"]

Is_DM = config["Debt_mapping"]


# ========================================
# HELPER FUNCTION: Apply All Filters
# ========================================
def apply_all_filters(df, display_columns, show_unmapped=False):
    """Apply all active filters to dataframe efficiently"""
    filtered = df.copy()
    
    # Apply unmapped filter
    if show_unmapped:
        unmapped_mask = filtered[METRIC_COLUMNS].apply(
            lambda row: all(pd.isna(val) or str(val).strip() == '' for val in row),
            axis=1
        )
        filtered = filtered[unmapped_mask]
    
    # Apply all display column filters - FIXED KEY
    for display_col in display_columns:
        if st.session_state.coa_applied_filters.get(display_col):  # ✅ Changed from applied_filters
            filtered = filtered[
                filtered[display_col].isin(st.session_state.coa_applied_filters[display_col])
            ]
    
    return filtered


# Callback functions for preview component
def cleanup_ui_edits():
    """Cleanup after UI edits are saved"""
    st.session_state.edited_rows = {}
    st.session_state.show_preview = False
    st.session_state.changes_to_apply = None


def cleanup_csv_upload():
    """Cleanup after CSV upload is saved"""
    st.session_state.show_preview = False
    st.session_state.changes_to_apply = None
    if 'bulk_upload' in st.session_state:
        del st.session_state['bulk_upload']


# Page title
st.title(f"Chart of Account Mapping Editor - {company}")
st.markdown("<style>h1 {font-size: 1.8rem !important;}</style>", unsafe_allow_html=True)

try:
    # ✨ LOAD DATA WITH SMART CASCADE CACHING
    metric_hierarchy_df = load_metric_hierarchy(METRIC_HIERARCHY_TABLE)
    cashflow_hierarchy_df = load_cashflow_hierarchy('CASHFLOW_HIERARCHY')
    actual_df = load_actual_data(TABLE_NAME, PRIMARY_KEYS, DISPLAY_COLUMNS)
    DEBT_MAPPING_OPTIONS = load_debt_mapping()
    
    # ✨ BUILD CASCADES WITH VERSION-BASED CACHING
    metric_tree = load_metric_cascade(company, METRIC_HIERARCHY_TABLE, metric_hierarchy_df)
    cashflow_tree = load_cashflow_cascade(cashflow_hierarchy_df)
    

    # Custom tab selector
    tab_option = st.radio(
        "Select Tab",
        ["📝 Update via UI", "📤 Update via CSV", "📚 Metric Hierarchy Reference", "📚 Cashflow Hierarchy Reference"],
        index=st.session_state.active_tab,
        horizontal=True,
        label_visibility="collapsed",
        key="tab_selector"
    )
    
    # Update active_tab
    tab_index = ["📝 Update via UI", "📤 Update via CSV", "📚 Metric Hierarchy Reference", "📚 Cashflow Hierarchy Reference"].index(tab_option)
    if tab_index != st.session_state.active_tab:
        st.session_state.active_tab = tab_index
        st.rerun()
    
    st.divider()
    
    # ========================================
    # TAB 1: UPDATE VIA UI
    # ========================================
    if st.session_state.active_tab == 0:
        st.subheader("🔍 Filters")
        show_unmapped = st.checkbox("🔍 Show Unmapped Accounts Only (L1-L6 empty)",
        value=st.session_state.show_unmapped_only,
        key="unmapped_filter_checkbox"  # Add unique key
        )

        if show_unmapped != st.session_state.show_unmapped_only:
            st.session_state.show_unmapped_only = show_unmapped
            st.session_state.page_offset = 0
            st.rerun()  # 🔥 KEY FIX: Force immediate rerun
        
        # # Render cascading filters
        # render_cascading_filters(
        #     coa_df=actual_df,
        #     display_columns=DISPLAY_COLUMNS,
        #     is_adj=False,
        #     show_unmapped=show_unmapped
        # )
        render_multiselect_filters(
            display_columns=DISPLAY_COLUMNS,
            reference_df=actual_df,
            applied_filters_key='coa_applied_filters',
            temp_filters_key='coa_temp_filters',
            filter_version_key='coa_filter_version'
        )

        
        # Clear all filters button
        render_clear_all_filters_button(
            display_columns=DISPLAY_COLUMNS,
            applied_filters_key='coa_applied_filters',
            temp_filters_key='coa_temp_filters',
            filter_version_key='coa_filter_version'
        )
        
        st.divider()
        
        # Apply filters
        filtered_df_tab1 = apply_all_filters(actual_df, DISPLAY_COLUMNS, show_unmapped)
        
        # Calculate pagination
        total_filtered_rows = len(filtered_df_tab1)
        total_pages = max(1, (total_filtered_rows + ROWS_PER_PAGE - 1) // ROWS_PER_PAGE)
        current_page = (st.session_state.page_offset // ROWS_PER_PAGE) + 1
        
        # Render metrics
        csv_data = convert_df_to_csv(filtered_df_tab1)
        render_table_metrics(
            total_rows=total_filtered_rows,
            current_page=current_page,
            total_pages=total_pages,
            download_data=csv_data,
            download_filename=f"coa_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        
        # Pagination controls
        render_pagination_controls(
            current_page=current_page,
            total_pages=total_pages,
            page_offset_key="page_offset",
            rows_per_page=ROWS_PER_PAGE
        )
        
        st.divider()
        
        # Display editable table
        start_idx = st.session_state.page_offset
        end_idx = min(start_idx + ROWS_PER_PAGE, total_filtered_rows)
        page_df = filtered_df_tab1.iloc[start_idx:end_idx]
        
        st.markdown('<div class="table-container">', unsafe_allow_html=True)
        
        # Render headers
        render_table_headers(DISPLAY_COLUMNS + list(MAPPING_COLS))
        
        st.markdown("---")
        
        # EDITABLE ROWS SECTION
        with st.container(height=600):
            for idx, row in page_df.iterrows():
                pk_values = tuple(row[pk] for pk in PRIMARY_KEYS)
                row_key = get_row_key(idx, *pk_values)
                pk_dict = {pk: row[pk] for pk in PRIMARY_KEYS}
                
                current_values = get_current_values(row, PRIMARY_KEYS, MAPPING_COLS)
                
                cols = st.columns([1.5] * len(DISPLAY_COLUMNS) + [1.25] * len(MAPPING_COLS))
                
                # Display columns
                for i, display_col in enumerate(DISPLAY_COLUMNS):
                    cols[i].markdown(f"<div class='data-cell'>{row[display_col]}</div>", unsafe_allow_html=True)
                
                col_offset = len(DISPLAY_COLUMNS)
                
                #  METRIC CASCADING DROPDOWNS - USING TREE
                
                # L1
                l1_options = get_options_from_tree(metric_tree)
                l1_index = l1_options.index(current_values['METRIC_L1']) if current_values['METRIC_L1'] in l1_options else 0
                l1_value = cols[col_offset].selectbox("L1", l1_options, index=l1_index,
                                                     key=f"l1_{row_key}", label_visibility="collapsed",
                                                     format_func=format_value_display)
                
                # L2
                l2_options = get_options_from_tree(metric_tree, l1_value)
                l2_index = l2_options.index(current_values['METRIC_L2']) if current_values['METRIC_L2'] in l2_options else 0
                l2_value = cols[col_offset+1].selectbox("L2", l2_options, index=l2_index,
                                                     key=f"l2_{row_key}", label_visibility="collapsed",
                                                     disabled=len(l2_options)==0,
                                                     format_func=format_value_display)
                
                # L3
                l3_options = get_options_from_tree(metric_tree, l1_value, l2_value)
                l3_index = l3_options.index(current_values['METRIC_L3']) if current_values['METRIC_L3'] in l3_options else 0
                l3_value = cols[col_offset+2].selectbox("L3", l3_options, index=l3_index,
                                                     key=f"l3_{row_key}", label_visibility="collapsed",
                                                     disabled=len(l3_options)==0,
                                                     format_func=format_value_display)
                
                # L4
                l4_options = get_options_from_tree(metric_tree, l1_value, l2_value, l3_value)
                l4_index = l4_options.index(current_values['METRIC_L4']) if current_values['METRIC_L4'] in l4_options else 0
                l4_value = cols[col_offset+3].selectbox("L4", l4_options, index=l4_index,
                                                     key=f"l4_{row_key}", label_visibility="collapsed",
                                                     disabled=len(l4_options)==0,
                                                     format_func=format_value_display)
                
                # L5
                l5_options = get_options_from_tree(metric_tree, l1_value, l2_value, l3_value, l4_value)
                l5_index = l5_options.index(current_values['METRIC_L5']) if current_values['METRIC_L5'] in l5_options else 0
                l5_value = cols[col_offset+4].selectbox("L5", l5_options, index=l5_index,
                                                     key=f"l5_{row_key}", label_visibility="collapsed",
                                                     disabled=len(l5_options)==0,
                                                     format_func=format_value_display)
                
                # L6
                l6_options = get_options_from_tree(metric_tree, l1_value, l2_value, l3_value, l4_value, l5_value)
                l6_index = l6_options.index(current_values['METRIC_L6']) if current_values['METRIC_L6'] in l6_options else 0
                l6_value = cols[col_offset+5].selectbox("L6", l6_options, index=l6_index,
                                                     key=f"l6_{row_key}", label_visibility="collapsed",
                                                     disabled=len(l6_options)==0,
                                                     format_func=format_value_display)
                
                # IS_BS
                IS_BS_options = ['', 'IS', 'BS']
                IS_BS_index = IS_BS_options.index(current_values['IS_BS']) if current_values['IS_BS'] in IS_BS_options else 0
                IS_BS_value = cols[col_offset+6].selectbox("IBS", IS_BS_options, index=IS_BS_index,
                                                     key=f"IS_BS_{row_key}", label_visibility="collapsed",
                                                     format_func=format_value_display)
                
                #  CASHFLOW CASCADING - USING TREE
                
                # CF_L1
                cf_l1_options = get_options_from_tree(cashflow_tree) if IS_BS_value == 'BS' else []
                cf_l1_index = cf_l1_options.index(current_values['CASHFLOW_L1']) if current_values['CASHFLOW_L1'] in cf_l1_options else 0
                cf_l1_value = cols[col_offset+7].selectbox("cf_l1", cf_l1_options, index=cf_l1_index,
                                                     key=f"cf_l1_{row_key}", label_visibility="collapsed",
                                                     disabled=len(cf_l1_options)==0,
                                                     format_func=format_value_display)
                
                # CF_L2
                cf_l2_options = get_options_from_tree(cashflow_tree, cf_l1_value) if IS_BS_value == 'BS' else []
                cf_l2_index = cf_l2_options.index(current_values['CASHFLOW_L2']) if current_values['CASHFLOW_L2'] in cf_l2_options else 0
                cf_l2_value = cols[col_offset+8].selectbox("cf_l2", cf_l2_options, index=cf_l2_index,
                                                     key=f"cf_l2_{row_key}", label_visibility="collapsed",
                                                     disabled=len(cf_l2_options)==0,
                                                     format_func=format_value_display)
                
                # CF_L3
                cf_l3_options = get_options_from_tree(cashflow_tree, cf_l1_value, cf_l2_value) if IS_BS_value == 'BS' else []
                cf_l3_index = cf_l3_options.index(current_values['CASHFLOW_L3']) if current_values['CASHFLOW_L3'] in cf_l3_options else 0
                cf_l3_value = cols[col_offset+9].selectbox("cf_l3", cf_l3_options, index=cf_l3_index,
                                                     key=f"cf_l3_{row_key}", label_visibility="collapsed",
                                                     disabled=len(cf_l3_options)==0,
                                                     format_func=format_value_display)
                
                # Debt mapping
                debt_options = DEBT_MAPPING_OPTIONS if l1_value in ('Current Liabilities', 'Non-Current Liabilities') and Is_DM else []
                debt_index = debt_options.index(current_values['DEBT_MAPPING']) if current_values['DEBT_MAPPING'] in debt_options else 0
                debt_value = cols[col_offset+10].selectbox("DM", debt_options, index=debt_index,
                                                     key=f"DM_{row_key}", label_visibility="collapsed",
                                                     disabled=len(debt_options)==0,
                                                     format_func=format_value_display)
                
                # Save edits
                edited_values = {
                    'METRIC_L1': normalize(l1_value),
                    'METRIC_L2': normalize(l2_value),
                    'METRIC_L3': normalize(l3_value),
                    'METRIC_L4': normalize(l4_value),
                    'METRIC_L5': normalize(l5_value),
                    'METRIC_L6': normalize(l6_value),
                    'IS_BS': normalize(IS_BS_value),
                    'CASHFLOW_L1': normalize(cf_l1_value),
                    'CASHFLOW_L2': normalize(cf_l2_value),
                    'CASHFLOW_L3': normalize(cf_l3_value),
                    'DEBT_MAPPING': normalize(debt_value)
                }
                
                save_edit(pk_dict, edited_values, PRIMARY_KEYS)
                
                st.markdown("<div style='margin: 2px 0;'></div>", unsafe_allow_html=True)
        
        st.markdown("</div>", unsafe_allow_html=True)
        
        # Preview Changes Button
        if st.button("Preview Changes", use_container_width=False):
            if not st.session_state.edited_rows:
                st.info("No changes detected.")
            else:
                changes = generate_changes_from_edited_rows(
                    st.session_state.edited_rows,
                    actual_df,
                    PRIMARY_KEYS,
                    MAPPING_COLS
                )
                
                if changes:
                    st.session_state.changes_to_apply = changes
                    st.session_state.show_preview = True
                else:
                    st.info("No actual changes detected (values match original).")
        
        # Preview Modal
        if st.session_state.show_preview and st.session_state.changes_to_apply:
            render_preview_and_confirm(
                changes=st.session_state.changes_to_apply,
                table_name=TABLE_NAME,
                primary_keys=PRIMARY_KEYS,
                mapping_cols=MAPPING_COLS,
                on_success_callback=cleanup_ui_edits,
                confirm_button_key="confirm_ui_update",
                cancel_button_key="cancel_ui_update"
            )
    
    # ========================================
    # TAB 2: CSV UPLOAD
    # ========================================
    elif st.session_state.active_tab == 1:
        st.header("Bulk Update from CSV")
        
        col1, col2 = st.columns([3, 3])
        with col1:
            template_csv = create_csv_template(PRIMARY_KEYS, MAPPING_COLS)
            st.download_button(
                label="Download CSV Template",
                data=template_csv,
                file_name="coa_template.csv",
                mime="text/csv",
                use_container_width=True
            )
        with col2:
            full_data_csv = convert_df_to_csv(actual_df)
            st.download_button(
                label="Download Full Data",
                data=full_data_csv,
                file_name=f"coa_full_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        st.divider()
        
        uploaded_file = st.file_uploader("Upload CSV file", type=["csv"], key="bulk_upload")
        
        # In Tab 2 (CSV Upload section), replace the validation block with this:

        if uploaded_file:
            try:
                csv_df = pd.read_csv(uploaded_file, dtype=str)
                
                required_cols = PRIMARY_KEYS + MAPPING_COLS
                missing = [c for c in required_cols if c not in csv_df.columns]
                
                if missing:
                    st.error(f"Missing required columns: {missing}")
                else:
                    # Check for duplicates BEFORE normalization
                    duplicate_mask = csv_df.duplicated(subset=PRIMARY_KEYS, keep=False)
                    
                    if duplicate_mask.any():
                        duplicate_rows = csv_df[duplicate_mask]
                        st.error(f"❌ Found {len(duplicate_rows)} rows with duplicate primary key combinations!")
                        st.dataframe(duplicate_rows)
                        st.stop()
                    else:
                        st.success("✅ File uploaded successfully!")
                        st.dataframe(csv_df.head(20))
                        
                        #  NORMALIZE CSV DATA FIRST
                        from utils.validators import normalize_csv_data
                        
                        csv_df_normalized = normalize_csv_data(csv_df, PRIMARY_KEYS, MAPPING_COLS)
                        
                        # Validate hierarchy (using normalized data)
                        hierarchy_errors, valid_rows = validate_csv_hierarchy(
                            csv_df_normalized,  # ← Use normalized data
                            metric_hierarchy_df,
                            cashflow_hierarchy_df,
                            DEBT_MAPPING_OPTIONS,
                            Is_DM,
                            PRIMARY_KEYS
                        )
                        
                        if hierarchy_errors:
                            st.error("❌ CSV hierarchy validation failed:")
                            for e in hierarchy_errors:
                                st.markdown(e)
                        else:
                            st.success(f"✅ All validations passed! {len(valid_rows)} rows validated.")
                            
                            # Generate changes (using normalized data)
                            changes, rows_to_update_df =  generate_changes_from_csv(
                                    csv_df_normalized,  # Already a DataFrame
                                    actual_df,
                                    PRIMARY_KEYS,
                                    MAPPING_COLS
                                )
                            
                            if not changes:
                                st.info("ℹ️ No changes detected (all values match existing data).")
                            else:
                                st.write(f"**{len(changes)} row(s) with changes detected**")
                                
                                #  Same preview function!
                                render_preview_and_confirm(
                                    changes=changes,
                                    table_name=TABLE_NAME,
                                    primary_keys=PRIMARY_KEYS,
                                    mapping_cols=MAPPING_COLS,
                                    on_success_callback=cleanup_csv_upload,
                                    confirm_button_key="confirm_csv_update",
                                    cancel_button_key="cancel_csv_update"
                                )
            
            except Exception as e:
                st.error(f"❌ Error reading file: {e}")
                st.exception(e)
    
   
    # ========================================
    # TAB 3: METRIC HIERARCHY REFERENCE
    # ========================================
    elif st.session_state.active_tab == 2:
      
        # Load fresh data from DB
        db_df = session.table(METRIC_HIERARCHY_TABLE).to_pandas()
        original_df = db_df.copy()
        
        # Render editable grid
        edited_df, has_changes = render_hierarchy_editor(
            table_name=METRIC_HIERARCHY_TABLE,
            hierarchy_df=db_df,
            original_df=original_df,
            title="Metric Hierarchy Reference",
            caption=f"Data from {METRIC_HIERARCHY_TABLE} table",
            session_state_keys={
                'new_rows': 'new_rows_metric_hierarchy',
                'csv_processed': 'csv_processed_metric_hierarchy',
                'uploaded_data': 'uploaded_data_metric_hierarchy',
                'last_file_id': 'last_file_id_metric_hierarchy'
            }
        )
        
        st.divider()
        
        # Save Changes button
        if st.button("💾 Save Changes", type="primary", key="save_metric_hierarchy"):
            new_row_ids = [row['ROW_ID'] for row in st.session_state.get('new_rows_metric_hierarchy', []) if row.get('ROW_ID')]
            
            success, message, inserts, updates = save_hierarchy_changes(
                edited_df=edited_df,
                original_df=original_df,
                table_name=METRIC_HIERARCHY_TABLE,
                session=session,
                new_row_ids_list=new_row_ids
            )
            
            if success:
                if inserts > 0 or updates > 0:
                    st.success(f"✅ {message}")
                    
                    # Clear cascade cache
                    from utils.cascade_helper import invalidate_cascade_cache
                    st.info("🔄 Clearing cascade cache...")
                    invalidate_cascade_cache()
                    
                    time.sleep(2)
                    
                    # Clear session state
                    st.session_state.new_rows_metric_hierarchy = []
                    st.session_state.csv_processed_metric_hierarchy = False
                    if 'uploaded_data_metric_hierarchy' in st.session_state:
                        del st.session_state.uploaded_data_metric_hierarchy
                    if 'last_file_id_metric_hierarchy' in st.session_state:
                        del st.session_state.last_file_id_metric_hierarchy
                    
                    st.rerun()
                else:
                    st.info("ℹ️ No changes to save")
            else:
                st.error(f"❌ {message}")
    
    # ========================================
    # TAB 4: CASHFLOW HIERARCHY REFERENCE
    # ========================================
    elif st.session_state.active_tab == 3:
    
        
        # Load fresh data from DB
        db_df = session.table('CASHFLOW_HIERARCHY').to_pandas()
        original_df = db_df.copy()
        
        # Render editable grid
        edited_df, has_changes = render_hierarchy_editor(
            table_name='CASHFLOW_HIERARCHY',
            hierarchy_df=db_df,
            original_df=original_df,
            title="Cashflow Hierarchy Reference",
            caption="Data from CASHFLOW_HIERARCHY table",
            session_state_keys={
                'new_rows': 'new_rows_cashflow_hierarchy',
                'csv_processed': 'csv_processed_cashflow_hierarchy',
                'uploaded_data': 'uploaded_data_cashflow_hierarchy',
                'last_file_id': 'last_file_id_cashflow_hierarchy'
            }
        )
        
        st.divider()
        
        # Save Changes button
        if st.button("💾 Save Changes", type="primary", key="save_cashflow_hierarchy"):
            new_row_ids = [row['ROW_ID'] for row in st.session_state.get('new_rows_cashflow_hierarchy', []) if row.get('ROW_ID')]
            
            success, message, inserts, updates = save_hierarchy_changes(
                edited_df=edited_df,
                original_df=original_df,
                table_name='CASHFLOW_HIERARCHY',
                session=session,
                new_row_ids_list=new_row_ids
            )
            
            if success:
                if inserts > 0 or updates > 0:
                    st.success(f"✅ {message}")
                    
                    # Clear cascade cache
                    from utils.cascade_helper import invalidate_cascade_cache
                    st.info("🔄 Clearing cascade cache...")
                    invalidate_cascade_cache()
                    
                    time.sleep(2)
                    
                    # Clear session state
                    st.session_state.new_rows_cashflow_hierarchy = []
                    st.session_state.csv_processed_cashflow_hierarchy = False
                    if 'uploaded_data_cashflow_hierarchy' in st.session_state:
                        del st.session_state.uploaded_data_cashflow_hierarchy
                    if 'last_file_id_cashflow_hierarchy' in st.session_state:
                        del st.session_state.last_file_id_cashflow_hierarchy
                    
                    st.rerun()
                else:
                    st.info("ℹ️ No changes to save")
            else:
                st.error(f"❌ {message}")

except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.exception(e)