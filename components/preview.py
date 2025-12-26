# components/preview.py
import streamlit as st
import pandas as pd
import time
from utils.data_loader import normalize, get_snowpark_session, get_current_user
from utils.formatters import format_change_display, build_preview_table_html, PREVIEW_TABLE_CSS


def generate_changes_from_edited_rows(edited_rows, original_df, primary_keys, mapping_cols):
    """
    Generate changes list from session state edited_rows
    
    Converts edited_rows dict into DataFrame, then calls generate_changes_from_csv
    
    Args:
        edited_rows: Dict of edited rows from session state
        original_df: Original dataframe to compare against
        primary_keys: List of primary key column names
        mapping_cols: List of mapping column names to compare
    
    Returns:
        List of change dictionaries
    """
    # Build edited dataframe from session state
    edited_data = []
    for edit_key, edited_values in edited_rows.items():
        pk_dict = {pk: edit_key[i] for i, pk in enumerate(primary_keys)}
        row_data = pk_dict.copy()
        row_data.update(edited_values)
        edited_data.append(row_data)
    
    if not edited_data:
        return []
    
    edited_df = pd.DataFrame(edited_data)
    
    # Use unified change detection
    changes, _ = generate_changes_from_csv(edited_df, original_df, primary_keys, mapping_cols)
    
    return changes


def generate_changes_from_csv(new_df, original_df, primary_keys, mapping_cols):
    """
    UNIFIED function to generate changes from any source (UI edits OR CSV)
    
    Args:
        new_df: DataFrame with new values (from UI edits or CSV)
        original_df: Original dataframe to compare against
        primary_keys: List of primary key column names
        mapping_cols: List of mapping column names to compare
    
    Returns:
        Tuple: (changes_list, rows_to_update_df)
    """
    # Normalize original_df
    original_df = original_df.copy()
    
   
    for col in mapping_cols:
        if col in original_df.columns:
            original_df[col] = original_df[col].fillna("").astype(str).str.strip()
    
    # Group original data by primary keys
    mapping_groups = original_df.groupby(primary_keys, dropna=False).agg({
        **{col: 'first' for col in mapping_cols if col in original_df.columns}
    }).reset_index()
    
    
    
    # Merge new data with original
    merge_cols = [col for col in (primary_keys + mapping_cols) if col in new_df.columns]
    merged = new_df[merge_cols].merge(
        mapping_groups,
        on=primary_keys,
        suffixes=("_new", "_old"),
        how="left"
    )
    
    # Detect changes
    changes = []
    
    for idx, row in merged.iterrows():
        diffs = {}
        has_change = False
        
        for col in mapping_cols:
            if col not in new_df.columns:
                continue
                
            old_val = row.get(f"{col}_old", "")
            new_val = row.get(f"{col}_new", "")
            
            old_str = "" if pd.isna(old_val) or old_val == "" else str(old_val).strip()
            new_str = "" if pd.isna(new_val) or new_val == "" else str(new_val).strip()
            
            if old_str != new_str:
                diffs[col] = {
                    'before': old_str if old_str else "[NULL/Blank]",
                    'after': new_str if new_str else "[NULL/Blank]"
                }
                has_change = True
        
        if has_change:
            pk_data = {pk: row[pk] for pk in primary_keys}
            
            change_info = {
                'primary_keys': pk_data,
                'changed_fields': diffs,
                'full_row': pk_data.copy()
            }
            
            for col in mapping_cols:
                if col in new_df.columns:
                    change_info['full_row'][col] = row.get(f"{col}_new", "")
            
            changes.append(change_info)
    
    # Build update dataframe
    if changes:
        rows_to_update_df = new_df[merge_cols].copy()
    else:
        rows_to_update_df = pd.DataFrame(columns=merge_cols)
    
    return changes, rows_to_update_df




def render_preview_modal(changes, primary_keys, mapping_cols, preview_limit=10):
    """
    Render the preview modal showing changes with Show More/Show Less
    
    Args:
        changes: List of change dictionaries
        primary_keys: List of primary key column names
        mapping_cols: List of mapping column names
        preview_limit: Number of rows to show initially (default 10)
    """
    st.markdown("### 📋 Preview Changes")
    
    total_changes = len(changes)
    st.markdown(f"**{total_changes} row(s) will be updated:**")
    
    # Build preview data
    preview_rows = []
    for change in changes:
        row_data = {}
        
        # Add primary keys
        pk_dict = change['primary_keys']
        for pk in primary_keys:
            row_data[pk] = pk_dict.get(pk, '')
        
        # Add mapping columns with before/after formatting
        for col in mapping_cols:
            if col in change['changed_fields']:
                before_val = change['changed_fields'][col]['before']
                after_val = change['changed_fields'][col]['after']
                row_data[col] = format_change_display(before_val, after_val)
            else:
                original_val = change['full_row'].get(col, "")
                row_data[col] = original_val if original_val else "[NULL/Blank]"
        
        preview_rows.append(row_data)
    
    preview_df = pd.DataFrame(preview_rows)
    
    # Initialize session state for show/hide
    if 'show_all_preview' not in st.session_state:
        st.session_state.show_all_preview = False
    
    # Determine how many rows to show
    if st.session_state.show_all_preview or total_changes <= preview_limit:
        display_df = preview_df
        rows_shown = total_changes
    else:
        display_df = preview_df.head(preview_limit)
        rows_shown = preview_limit
    
    # Display preview table
    st.markdown(PREVIEW_TABLE_CSS, unsafe_allow_html=True)
    html_table = build_preview_table_html(display_df)
    st.markdown(html_table, unsafe_allow_html=True)
    
    # Show More / Show Less toggle (only if there are more rows than limit)
    if total_changes > preview_limit:
        st.caption(f"Showing {rows_shown} of {total_changes} changes")
        
        col1, col2, col3 = st.columns([1, 1, 4])
        
        with col1:
            if not st.session_state.show_all_preview:
                if st.button(f"📖 Show All ({total_changes})", key="show_more_preview"):
                    st.session_state.show_all_preview = True
                    st.rerun()
            else:
                if st.button(f"📕 Show Less ({preview_limit})", key="show_less_preview"):
                    st.session_state.show_all_preview = False
                    st.rerun()
    
    st.divider()
def execute_merge_operation(changes, table_name, primary_keys, mapping_cols, 
                            display_columns=None, use_upsert=False):
    """
    UNIFIED MERGE function for both UPDATE and UPSERT operations
    
    Args:
        changes: List of change dictionaries
        table_name: Name of the table to update
        primary_keys: Primary keys for matching
        mapping_cols: Columns to insert/update
        display_columns: Optional display columns to include (for Adjustments)
        use_upsert: If True, INSERT + UPDATE. If False, UPDATE only
    
    Returns:
        Tuple: (success, message, inserts, updates)
    """
    session = get_snowpark_session()
    current_user = get_current_user()
    temp_table = None
    
    if not changes:
        return True, "No changes to process", 0, 0
    
    try:
        from datetime import datetime
        
        # Build dataframe from changes
        update_data = []
        for change in changes:
            row_data = change['primary_keys'].copy()
            
            # Add mapping columns
            for col in mapping_cols:
                row_data[col] = change['full_row'].get(col, '')
            
            # Add display columns if provided (for Adjustments)
            if display_columns:
                for col in display_columns:
                    row_data[col] = change['full_row'].get(col, '')
            
            update_data.append(row_data)
        
        update_df = pd.DataFrame(update_data)
        
        # Create temporary table
        temp_table = f"TEMP_MERGE_{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        
        # Write data to temp table
        session.write_pandas(
            update_df,
            temp_table,
            auto_create_table=True,
            overwrite=True
        )
        
        # Build MERGE query
        match_conditions = [f"TARGET.{pk} = SOURCE.{pk}" for pk in primary_keys]
        
        # SET clause for UPDATE
        set_clauses = [f"TARGET.{col} = SOURCE.{col}" for col in mapping_cols]
        
        # Add display columns to SET if provided
        if display_columns:
            for col in display_columns:
                set_clauses.append(f"TARGET.{col} = SOURCE.{col}")
        
        # Add audit fields
        current_timestamp = session.sql("SELECT CURRENT_TIMESTAMP()").collect()[0][0]
        set_clauses.append(f"TARGET.LAST_UPDATED_BY = '{current_user}'")
        set_clauses.append(f"TARGET.LAST_UPDATED_AT = '{current_timestamp}'")
        
        # Build MERGE query
        merge_query = f"""
            MERGE INTO {table_name} TARGET
            USING {temp_table} SOURCE
            ON {' AND '.join(match_conditions)}
            WHEN MATCHED THEN 
                UPDATE SET {', '.join(set_clauses)}
        """
        
        # Add INSERT clause if upsert mode
        if use_upsert:
            all_cols = primary_keys + mapping_cols
            if display_columns:
                all_cols += display_columns
            
            insert_cols = ', '.join(all_cols) + ', CREATED_BY, CREATED_AT'
            insert_vals = ', '.join([f"SOURCE.{col}" for col in all_cols])
            insert_vals += f", '{current_user}', '{current_timestamp}'"
            
            merge_query += f"""
            WHEN NOT MATCHED THEN 
                INSERT ({insert_cols})
                VALUES ({insert_vals})
            """
        
        # Execute MERGE
        result = session.sql(merge_query).collect()
        
        # Parse result
        inserts = 0
        updates = 0
        
        if result and len(result) > 0:
            result_dict = result[0].as_dict()
            inserts = result_dict.get('number of rows inserted', 0)
            updates = result_dict.get('number of rows updated', 0)
        
        # If no result metadata, assume all were updates
        if inserts == 0 and updates == 0:
            updates = len(changes)
        
        # Drop temp table
        session.sql(f"DROP TABLE IF EXISTS {temp_table}").collect()
        temp_table = None
        
        # Build message
        messages = []
        if inserts > 0:
            messages.append(f"Inserted {inserts} row(s)")
        if updates > 0:
            messages.append(f"Updated {updates} row(s)")
        
        message = " | ".join(messages) if messages else "No changes"
        
        return True, message, inserts, updates
        
    except Exception as e:
        # Drop temp table on error
        if temp_table:
            try:
                session.sql(f"DROP TABLE IF EXISTS {temp_table}").collect()
            except:
                pass
        
        return False, f"Error during merge: {str(e)}", 0, 0


def render_preview_and_confirm(
    changes,
    table_name,
    primary_keys,
    mapping_cols,
    display_columns=None,
    on_success_callback=None,
    confirm_button_key="confirm_update",
    cancel_button_key="cancel_update",
    use_upsert=False
):
    """
    UNIFIED preview and confirm workflow for COA and Adjustments
    
    Args:
        changes: List of change dictionaries
        table_name: Name of table to update
        primary_keys: Primary keys for matching
        mapping_cols: Columns to update
        display_columns: Optional display columns (for Adjustments)
        on_success_callback: Function to call after successful update
        confirm_button_key: Unique key for confirm button
        cancel_button_key: Unique key for cancel button
        use_upsert: If True, use UPSERT (INSERT or UPDATE), else just UPDATE
    """
    # Render preview
    render_preview_modal(changes, primary_keys, mapping_cols)
    
    # Action buttons
    action_cols = st.columns([1, 1, 4])
    
    with action_cols[0]:
        if st.button("✅ Confirm & Update", type="primary", key=confirm_button_key):
            with st.spinner("Updating database..."):
                success, message, inserts, updates = execute_merge_operation(
                    changes=changes,
                    table_name=table_name,
                    primary_keys=primary_keys,
                    mapping_cols=mapping_cols,
                    display_columns=display_columns,
                    use_upsert=use_upsert
                )
            
            if success:
                st.toast(f"✅ {message}", icon="✅")
                
                if on_success_callback:
                    on_success_callback()
                
                st.cache_data.clear()
                time.sleep(1)
                st.rerun()
            else:
                st.error(f"❌ {message}")
    
    with action_cols[1]:
        if st.button("❌ Cancel", key=cancel_button_key):
            if on_success_callback:
                on_success_callback()
            st.rerun()