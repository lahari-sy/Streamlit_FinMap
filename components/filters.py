import streamlit as st


def render_multiselect_filters(
    display_columns,
    reference_df,
    applied_filters_key,
    temp_filters_key,
    filter_version_key,
    on_filter_change_callback=None
):
    """
    Render instant-apply cascading multiselect filters with search
    """
    # Initialize session state - ROBUST VERSION
    if applied_filters_key not in st.session_state:
        st.session_state[applied_filters_key] = {col: [] for col in display_columns}
    else:
        # Ensure all current display_columns exist
        for col in display_columns:
            if col not in st.session_state[applied_filters_key]:
                st.session_state[applied_filters_key][col] = []
    
    if temp_filters_key not in st.session_state:
        st.session_state[temp_filters_key] = {col: [] for col in display_columns}
    else:
        # Ensure all current display_columns exist
        for col in display_columns:
            if col not in st.session_state[temp_filters_key]:
                st.session_state[temp_filters_key][col] = []
    
    if filter_version_key not in st.session_state:
        st.session_state[filter_version_key] = 0
    
    # Create filter columns
    filter_cols = st.columns(len(display_columns))
    
    for i, col in enumerate(display_columns):
        if col not in reference_df.columns:
            st.error(f"Column '{col}' not found in reference data")
            continue
        
        # Apply previously applied filters for cascading
        filtered_ref = reference_df.copy()
        for j, other_col in enumerate(display_columns):
            if j < i and st.session_state[applied_filters_key].get(other_col):  # Safe .get()
                filtered_ref = filtered_ref[
                    filtered_ref[other_col].isin(st.session_state[applied_filters_key][other_col])
                ]
        
        # Get unique values for this column
        unique_values = sorted(
            filtered_ref[col].dropna().astype(str).unique().tolist()
        )
        
        with filter_cols[i]:
            # Count of selected items - Safe access
            selected_count = len(st.session_state[applied_filters_key].get(col, []))
            filter_label = f"{col}" + (f" ({selected_count})" if selected_count > 0 else "")
            
            # Create popover for filter
            with st.popover(filter_label, use_container_width=True):
                st.markdown(f"**{col}**")
                
                # Search box
                search_query = st.text_input(
                    "🔍 Search",
                    key=f"search_{col}_{i}_v{st.session_state[filter_version_key]}",
                    placeholder="Type to filter...",
                    label_visibility="collapsed"
                )
                
                # Filter options based on search
                if search_query:
                    filtered_values = [v for v in unique_values if search_query.lower() in v.lower()]
                    st.caption(f"Showing {len(filtered_values)} of {len(unique_values)}")
                else:
                    filtered_values = unique_values
                
                # Quick action buttons
                col1, col2 = st.columns(2)
                
                if col1.button("✅ All", key=f"select_all_{col}_{i}", use_container_width=True):
                    if search_query:
                        for v in filtered_values:
                            if v not in st.session_state[applied_filters_key][col]:
                                st.session_state[applied_filters_key][col].append(v)
                    else:
                        st.session_state[applied_filters_key][col] = unique_values.copy()
                    
                    st.session_state[filter_version_key] += 1
                    st.session_state.page_offset = 0
                    
                    if on_filter_change_callback:
                        on_filter_change_callback()
                    st.rerun()
                
                if col2.button("❌ None", key=f"clear_all_{col}_{i}", use_container_width=True):
                    if search_query:
                        for v in filtered_values:
                            if v in st.session_state[applied_filters_key][col]:
                                st.session_state[applied_filters_key][col].remove(v)
                    else:
                        st.session_state[applied_filters_key][col] = []
                    
                    st.session_state[filter_version_key] += 1
                    st.session_state.page_offset = 0
                    
                    if on_filter_change_callback:
                        on_filter_change_callback()
                    st.rerun()
                
                st.divider()
                
                # Selected count indicator - SAFE ACCESS
                selected_in_view = len([v for v in filtered_values if v in st.session_state[applied_filters_key].get(col, [])])
                if selected_in_view > 0:
                    st.caption(f"✓ {selected_in_view} selected")
                
                # INSTANT APPLY CHECKBOXES
                if len(filtered_values) == 0:
                    st.info("No options match your search")
                else:
                    with st.container(height=250):
                        for value in filtered_values:
                            is_checked = value in st.session_state[applied_filters_key].get(col, [])  # Safe .get()
                            checkbox_key = f"cb_{col}_{value}_{i}_v{st.session_state[filter_version_key]}"
                            
                            new_state = st.checkbox(value, value=is_checked, key=checkbox_key)
                            
                            if new_state != is_checked:
                                if new_state:
                                    st.session_state[applied_filters_key][col].append(value)
                                else:
                                    st.session_state[applied_filters_key][col].remove(value)
                                
                                st.session_state.page_offset = 0
                                
                                if on_filter_change_callback:
                                    on_filter_change_callback()
                                st.rerun()

@st.fragment
def _render_filter_content(
    col,
    i,
    unique_values,
    applied_filters_key,
    filter_version_key,
    on_filter_change_callback=None
):
    """
    Fragment to handle filter content - keeps popover open during interactions
    """
    st.markdown(f"**Filter: {col}**")
    
    # Search box
    search_key = f"search_{col}_{i}_v{st.session_state[filter_version_key]}"
    search_query = st.text_input(
        "🔍 Search",
        key=search_key,
        placeholder="Type to filter options...",
        label_visibility="collapsed"
    )
    
    # Filter options based on search
    if search_query:
        filtered_values = [v for v in unique_values if search_query.lower() in v.lower()]
        st.caption(f"Showing {len(filtered_values)} of {len(unique_values)} options")
    else:
        filtered_values = unique_values
    
    # Quick action buttons
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("✅ All", key=f"select_all_{col}_{i}", use_container_width=True):
            if search_query:
                # Add filtered values
                for v in filtered_values:
                    if v not in st.session_state[applied_filters_key][col]:
                        st.session_state[applied_filters_key][col].append(v)
            else:
                st.session_state[applied_filters_key][col] = unique_values.copy()
            
            st.session_state.page_offset = 0  # Reset pagination
            
            if on_filter_change_callback:
                on_filter_change_callback()
            
            st.rerun()
    
    with col2:
        if st.button("❌ None", key=f"clear_all_{col}_{i}", use_container_width=True):
            if search_query:
                # Remove filtered values
                for v in filtered_values:
                    if v in st.session_state[applied_filters_key][col]:
                        st.session_state[applied_filters_key][col].remove(v)
            else:
                st.session_state[applied_filters_key][col] = []
            
            st.session_state.page_offset = 0
            
            if on_filter_change_callback:
                on_filter_change_callback()
            
            st.rerun()
    
    st.divider()
    
    # INSTANT-APPLY CHECKBOXES in scrollable container
    if len(filtered_values) == 0:
        st.info("No options match your search")
    else:
        # Show count of selected vs total
        selected_in_view = len([v for v in filtered_values if v in st.session_state[applied_filters_key][col]])
        st.caption(f"Selected: {selected_in_view}/{len(filtered_values)}")
        
        # Scrollable checkbox list
        with st.container(height=300):
            for value in filtered_values:
                is_checked = value in st.session_state[applied_filters_key][col]
                checkbox_key = f"cb_{col}_{value}_{i}_v{st.session_state[filter_version_key]}"
                
                # INSTANT APPLY: Update applied_filters directly
                new_state = st.checkbox(value, value=is_checked, key=checkbox_key)
                
                if new_state and not is_checked:
                    # Just checked
                    st.session_state[applied_filters_key][col].append(value)
                    st.session_state.page_offset = 0
                elif not new_state and is_checked:
                    # Just unchecked
                    st.session_state[applied_filters_key][col].remove(value)
                    st.session_state.page_offset = 0

def render_clear_all_filters_button(
    display_columns,
    applied_filters_key,
    temp_filters_key,
    filter_version_key,
    on_clear_callback=None
):
    """Render a 'Clear All Filters' button"""
    has_filters = any(st.session_state.get(applied_filters_key, {}).get(col, []) 
                     for col in display_columns)
    
    if has_filters:
        if st.button("🔄 Clear All Filters", use_container_width=False):
            st.session_state[applied_filters_key] = {col: [] for col in display_columns}
            st.session_state[temp_filters_key] = {col: [] for col in display_columns}
            st.session_state[filter_version_key] += 1
            st.session_state.page_offset = 0
            
            if on_clear_callback:
                on_clear_callback()
            
            st.rerun()

def apply_filters_to_dataframe(df, display_columns, applied_filters):
    """
    Apply filters to a dataframe
    
    Args:
        df: Dataframe to filter
        display_columns: List of column names to filter on
        applied_filters: Dict of {column: [selected_values]}
    
    Returns:
        Filtered dataframe
    """
    filtered_df = df.copy()
    
    for col in display_columns:
        if col in applied_filters and applied_filters[col]:
            filtered_df = filtered_df[filtered_df[col].isin(applied_filters[col])]
    
    return filtered_df