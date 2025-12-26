# components/hierarchy_editor.py
import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from datetime import datetime
from utils.data_loader import get_snowpark_session, get_current_user


def render_hierarchy_editor(
    table_name,
    hierarchy_df,
    original_df,
    title="Hierarchy Reference",
    caption=None,
    editable=True,
    allow_add_rows=True,
    allow_csv_upload=True,
    page_size=25,
    session_state_keys=None,
    primary_key='ROW_ID'  # ✅ PRIMARY KEY PARAMETER
):
    """
    Render an editable AG-Grid for hierarchy management
    
    Args:
        table_name: Name of the Snowflake table
        hierarchy_df: Current hierarchy DataFrame to display/edit
        original_df: Original DataFrame from DB (for comparison)
        title: Title to display
        caption: Optional caption below title
        editable: Whether grid is editable
        allow_add_rows: Whether to show "Add Row" button
        allow_csv_upload: Whether to show CSV upload
        page_size: Rows per page in grid
        session_state_keys: Dict with keys for session state
        primary_key: Name of primary key column (default: 'ROW_ID')
    
    Returns:
        Tuple: (edited_df, has_changes)
    """
    # Default session state keys if not provided
    if session_state_keys is None:
        safe_table_name = table_name.replace('.', '_').lower()
        session_state_keys = {
            'new_rows': f'new_rows_{safe_table_name}',
            'csv_processed': f'csv_processed_{safe_table_name}',
            'uploaded_data': f'uploaded_data_{safe_table_name}',
            'last_file_id': f'last_file_id_{safe_table_name}'
        }
    
    # Initialize session state
    if session_state_keys['new_rows'] not in st.session_state:
        st.session_state[session_state_keys['new_rows']] = []
    
    if session_state_keys['csv_processed'] not in st.session_state:
        st.session_state[session_state_keys['csv_processed']] = False
    
    # Display title and caption
    st.subheader(title)
    if caption:
        st.caption(caption)
    
    # Check if we're in CSV mode
    in_csv_mode = st.session_state.get(session_state_keys['csv_processed'], False)
    
    # Start with provided hierarchy_df
    display_df = hierarchy_df.copy()
    
    # Clean up new_rows - remove any that are now in DB
    if st.session_state[session_state_keys['new_rows']]:
        if primary_key in display_df.columns:
            existing_ids = set(display_df[primary_key].dropna().tolist())
            st.session_state[session_state_keys['new_rows']] = [
                row for row in st.session_state[session_state_keys['new_rows']]
                if row.get(primary_key) not in existing_ids
            ]
    
    # Append new rows from UI (only if NOT in CSV mode)
    if not in_csv_mode and st.session_state[session_state_keys['new_rows']]:
        new_rows_df = pd.DataFrame(st.session_state[session_state_keys['new_rows']])
        display_df = pd.concat([display_df, new_rows_df], ignore_index=True)
    
    # ============================================
    # TOP BUTTONS
    # ============================================
    
    if in_csv_mode:
        #  CSV MODE: Show clear button prominently
        button_cols = st.columns([2, 4])
        
        with button_cols[0]:
            if st.button("🔙 Clear CSV & Return to UI", type="secondary", key=f"clear_csv_{table_name}"):
                # Clear CSV-related session state
                st.session_state[session_state_keys['csv_processed']] = False
                if session_state_keys['uploaded_data'] in st.session_state:
                    del st.session_state[session_state_keys['uploaded_data']]
                if session_state_keys['last_file_id'] in st.session_state:
                    del st.session_state[session_state_keys['last_file_id']]
                st.rerun()
        
        st.info(f"📝 **CSV Mode Active** - Displaying uploaded CSV data. Click 'Clear CSV & Return to UI' to go back to normal editing mode.")
    
    else:
        # ✨ NORMAL MODE: Show add row and CSV upload buttons
        button_cols = st.columns([1, 1, 4])
        
        with button_cols[0]:
            if allow_add_rows and st.button("➕ Add New Row", key=f"add_row_{table_name}"):
                # Generate new ID for primary key
                if len(display_df) > 0 and primary_key in display_df.columns and not display_df[primary_key].isna().all():
                    max_id = display_df[primary_key].max()
                    new_id = int(max_id) + 1
                else:
                    new_id = 1
                
                # Create new row with all columns
                new_row = {col: None for col in display_df.columns}
                new_row[primary_key] = new_id
                new_row['LAST_UPDATED_AT'] = datetime.now()
                
                st.session_state[session_state_keys['new_rows']].append(new_row)
                st.rerun()
        
        with button_cols[1]:
            if allow_csv_upload:
                uploaded_file = st.file_uploader(
                    "📤 Upload CSV",
                    type=['csv'],
                    key=f"hierarchy_csv_{table_name}",
                    help=f"Upload CSV. Rows with existing {primary_key} will update, new {primary_key}s will insert."
                )
    
    # ============================================
    # HANDLE CSV UPLOAD
    # ============================================
    
    if allow_csv_upload and not in_csv_mode and 'uploaded_file' in locals() and uploaded_file is not None:
        file_id = f"{uploaded_file.name}_{uploaded_file.size}_{id(uploaded_file)}"
        
        if st.session_state.get(session_state_keys['last_file_id']) != file_id:
            try:
                with st.spinner("Processing CSV..."):
                    uploaded_df = pd.read_csv(
                        uploaded_file,
                        encoding='utf-8-sig',
                        on_bad_lines='skip',
                        low_memory=False
                    )
                    st.write(primary_key)
                    if primary_key not in uploaded_df.columns:
                        st.error(f"❌ CSV must contain {primary_key} column")
                    else:
                        # Clean data
                        uploaded_df[primary_key] = pd.to_numeric(uploaded_df[primary_key], errors='coerce')
                        uploaded_df = uploaded_df.dropna(subset=[primary_key])
                        uploaded_df[primary_key] = uploaded_df[primary_key].astype(int)
                        
                        # Match schema
                        for col in display_df.columns:
                            if col not in uploaded_df.columns:
                                uploaded_df[col] = None
                        uploaded_df = uploaded_df[display_df.columns]
                        
                        # Store in session state
                        st.session_state[session_state_keys['uploaded_data']] = uploaded_df.copy()
                        st.session_state[session_state_keys['last_file_id']] = file_id
                        st.session_state[session_state_keys['csv_processed']] = True
                        
                        st.success(f"✅ Loaded {len(uploaded_df)} rows from CSV")
                        st.rerun()  # ✨ Rerun to show CSV mode UI
            
            except Exception as e:
                st.error(f"❌ Error loading CSV: {str(e)}")
                st.session_state[session_state_keys['csv_processed']] = False
    
    # Use uploaded data if in CSV mode
    if in_csv_mode and session_state_keys['uploaded_data'] in st.session_state:
        display_df = st.session_state[session_state_keys['uploaded_data']].copy()
    
    # ============================================
    # AG-GRID CONFIGURATION
    # ============================================
    
    gb = GridOptionsBuilder.from_dataframe(display_df)
    
    # Pagination
    gb.configure_pagination(
        enabled=True,
        paginationAutoPageSize=False,
        paginationPageSize=page_size
    )
    
    # Sidebar
    gb.configure_side_bar()
    
    # Default column settings
    gb.configure_default_column(
        editable=editable,
        filter=True,
        sortable=True,
        resizable=True,
        groupable=True,
        value=True,
        enablePivot=True,
        minWidth=100
    )
    
    # Primary key should not be editable
    if primary_key in display_df.columns:
        gb.configure_column(primary_key, editable=False, sort='desc', pinned='left')
    
    # Grid options
    gb.configure_grid_options(
        enableRangeSelection=True,
        copyHeadersToClipboard=True,
        suppressCopyRowsToClipboard=False
    )
    
    grid_options = gb.build()
    
    # ============================================
    # RENDER AG-GRID
    # ============================================
    
    grid_response = AgGrid(
        display_df,
        gridOptions=grid_options,
        update_mode=GridUpdateMode.MODEL_CHANGED,
        fit_columns_on_grid_load=True,
        enable_enterprise_modules=False,
        height=600,
        theme="streamlit",
    )
    
    edited_df = grid_response['data']
    
    # ============================================
    # CAPTION AND DOWNLOAD
    # ============================================
    
    caption_cols = st.columns([5, 1])
    with caption_cols[0]:
        st.caption(f"Total Records: {len(display_df):,}")
    with caption_cols[1]:
        csv_data = edited_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download",
            data=csv_data,
            file_name=f"{table_name.lower()}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True,
            key=f"download_{table_name}"
        )
    
    # ✅ CORRECTED: Properly detect changes
    has_changes = False
    inserts = 0
    updates = 0
    
    # Check for new rows (inserts)
    if st.session_state[session_state_keys['new_rows']]:
        inserts = len(st.session_state[session_state_keys['new_rows']])
        has_changes = True
    
    # Check for updates in existing rows
    existing_ids = set(original_df[primary_key].dropna().tolist())
    
    for idx, edited_row in edited_df.iterrows():
        row_id = edited_row[primary_key]
        
        # Skip new rows (already counted as inserts)
        if row_id not in existing_ids:
            continue
        
        # Find original row
        original_row = original_df[original_df[primary_key] == row_id]
        
        if len(original_row) > 0:
            original_row = original_row.iloc[0]
            
            # Check if any value changed
            for col in edited_df.columns:
                if col in [primary_key, 'LAST_UPDATED_BY', 'LAST_UPDATED_AT']:
                    continue
                
                old_val = str(original_row[col]).strip() if pd.notna(original_row[col]) else ''
                new_val = str(edited_row[col]).strip() if pd.notna(edited_row[col]) else ''
                
                if old_val != new_val:
                    updates += 1
                    has_changes = True
                    break  # Found a change, move to next row
    
    return edited_df, has_changes



def save_hierarchy_changes(
    edited_df,
    original_df,
    table_name,
    session,
    new_row_ids_list=None,
    primary_key='ROW_ID'  # ✅ PRIMARY KEY PARAMETER
):
    """
    Save hierarchy changes to database using MERGE
    
    Args:
        edited_df: Edited dataframe
        original_df: Original dataframe from DB
        table_name: Name of the table
        session: Snowpark session
        new_row_ids_list: List of primary key values that are new (not yet in DB)
        primary_key: Name of primary key column (default: 'ROW_ID')
    
    Returns:
        Tuple: (success, message, inserts_count, updates_count)
    """
    from utils.data_loader import get_current_user
    
    current_user = get_current_user()
    temp_table = None
    
    try:
        from datetime import datetime
        
        if new_row_ids_list is None:
            new_row_ids_list = []
        
        # ============================================
        # FIND INSERTS
        # ============================================
        
        existing_ids = set(original_df[primary_key].dropna().tolist())
        inserts_df = edited_df[~edited_df[primary_key].isin(existing_ids)].copy()
        
        # ============================================
        # FIND UPDATES
        # ============================================
        
        existing_edited_df = edited_df[edited_df[primary_key].isin(existing_ids)]
        merged = existing_edited_df.merge(
            original_df,
            on=primary_key,
            how='inner',
            suffixes=('', '_orig')
        )
        
        changed_rows = []
        if not merged.empty:
            compare_cols = [col for col in edited_df.columns
                          if col not in [primary_key, 'LAST_UPDATED_BY', 'LAST_UPDATED_AT']]
            
            for idx, row in merged.iterrows():
                row_changed = False
                for col in compare_cols:
                    orig_col = f"{col}_orig"
                    if orig_col in merged.columns:
                        edited_val = row[col]
                        orig_val = row[orig_col]
                        
                        edited_str = str(edited_val).strip() if pd.notna(edited_val) else ""
                        orig_str = str(orig_val).strip() if pd.notna(orig_val) else ""
                        
                        if edited_str == "" and orig_str == "":
                            continue
                        elif edited_str != orig_str:
                            row_changed = True
                            break
                
                if row_changed:
                    changed_rows.append(idx)
            
            if changed_rows:
                updates_df = merged.loc[changed_rows, edited_df.columns].copy()
            else:
                updates_df = pd.DataFrame(columns=edited_df.columns)
        else:
            updates_df = pd.DataFrame(columns=edited_df.columns)
        
        # ============================================
        # WRITE INSERTS
        # ============================================
        
        inserts_count = 0
        if not inserts_df.empty:
            # Filter out empty rows
            non_empty_inserts = []
            for idx, row in inserts_df.iterrows():
                has_data = False
                for col in inserts_df.columns:
                    if col != primary_key and pd.notna(row[col]) and str(row[col]).strip() != "":
                        has_data = True
                        break
                if has_data:
                    non_empty_inserts.append(idx)
            
            if non_empty_inserts:
                inserts_df = inserts_df.loc[non_empty_inserts].copy()
                current_timestamp = session.sql("SELECT CURRENT_TIMESTAMP()").collect()[0][0]
                inserts_df['LAST_UPDATED_BY'] = current_user
                inserts_df['LAST_UPDATED_AT'] = current_timestamp
                
                session.write_pandas(
                    inserts_df,
                    table_name,
                    overwrite=False
                )
                inserts_count = len(inserts_df)
        
        # ============================================
        # WRITE UPDATES USING MERGE
        # ============================================
        
        updates_count = 0
        if not updates_df.empty:
            temp_table = f"TEMP_HIERARCHY_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            session.write_pandas(
                updates_df,
                temp_table,
                auto_create_table=True,
                overwrite=True
            )
            
            # Build SET clause (exclude primary key and audit columns)
            excluded_cols = [primary_key, 'LAST_UPDATED_BY', 'LAST_UPDATED_AT']
            set_clauses = [
                f"TARGET.{col} = SOURCE.{col}"
                for col in edited_df.columns
                if col not in excluded_cols
            ]
            
            merge_query = f"""
                MERGE INTO {table_name} TARGET
                USING {temp_table} SOURCE
                ON TARGET.{primary_key} = SOURCE.{primary_key}
                WHEN MATCHED THEN UPDATE SET
                    {', '.join(set_clauses)},
                    TARGET.LAST_UPDATED_BY = '{current_user}',
                    TARGET.LAST_UPDATED_AT = CURRENT_TIMESTAMP()
            """
            
            session.sql(merge_query).collect()
            
            # ✨ Drop temp table
            session.sql(f"DROP TABLE IF EXISTS {temp_table}").collect()
            temp_table = None
            
            updates_count = len(updates_df)
        
        # Build message
        messages = []
        if inserts_count > 0:
            messages.append(f"Inserted {inserts_count} row(s)")
        if updates_count > 0:
            messages.append(f"Updated {updates_count} row(s)")
        
        if messages:
            return True, " | ".join(messages), inserts_count, updates_count
        else:
            return True, "No changes detected", 0, 0
    
    except Exception as e:
        # ✨ Drop temp table on error
        if temp_table:
            try:
                session.sql(f"DROP TABLE IF EXISTS {temp_table}").collect()
            except:
                pass
        
        return False, f"Error saving changes: {str(e)}", 0, 0