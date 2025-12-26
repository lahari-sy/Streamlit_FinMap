# utils/cascade_helper.py
import streamlit as st
import pandas as pd
from datetime import datetime

# ============================================
# CACHE INVALIDATION HELPERS
# ============================================

def get_hierarchy_version(company, hierarchy_table):
    """
    Get a version identifier for the hierarchy
    This changes whenever the hierarchy is updated
    """
    from utils.data_loader import get_snowpark_session
    session = get_snowpark_session()
    
    try:
        query = f"""
            SELECT MAX(LAST_UPDATED_AT) as VERSION
            FROM {hierarchy_table}
        """
        result = session.sql(query).collect()
        
        if result and result[0]['VERSION']:
            return str(result[0]['VERSION'])
    except:
        try:
            query = f"SELECT COUNT(*) as CNT FROM {hierarchy_table}"
            result = session.sql(query).collect()
            if result:
                return f"count_{result[0]['CNT']}"
        except:
            pass
    
    return datetime.now().isoformat()


def invalidate_cascade_cache():
    """Force rebuild of all cascades"""
    st.cache_data.clear()


# ============================================
# TREE BUILDING
# ============================================

def build_metric_tree_from_df(df):
    """
    Build tree structure from metric hierarchy dataframe
    PRESERVES blank/null options as valid choices
    """
    # Clean data - convert NaN to empty string but DON'T remove blanks
    for i in range(1, 7):
        col = f'METRIC_L{i}'
        df[col] = df[col].fillna('').astype(str).str.strip()
    
    # Build tree - INCLUDING rows where values are blank
    tree = {'options': set(), 'children': {}}
    
    for _, row in df.iterrows():
        current_node = tree
        
        for level in range(1, 7):
            value = row[f'METRIC_L{level}']
            
            # Add to options (including blank)
            current_node['options'].add(value)
            
            # Create child node if doesn't exist
            if value not in current_node['children']:
                current_node['children'][value] = {
                    'options': set(),
                    'children': {}
                }
            
            # Move to child node
            current_node = current_node['children'][value]
    
    # Convert sets to sorted lists
    def convert_and_sort(node):
        # Convert set to list
        options_list = list(node['options'])
        
        # Sort with blank first
        if '' in options_list:
            options_list.remove('')
            sorted_options = [''] + sorted([opt for opt in options_list if opt])
        else:
            sorted_options = sorted([opt for opt in options_list if opt])
        
        node['options'] = sorted_options
        
        # Recursively process children
        for child in node['children'].values():
            convert_and_sort(child)
    
    convert_and_sort(tree)
    
    return tree


def build_cashflow_tree_from_df(df):
    """
    Build tree structure from cashflow hierarchy dataframe
    PRESERVES blank/null options as valid choices
    """
    # Clean data - convert NaN to empty string but DON'T remove blanks
    for i in range(1, 4):
        col = f'CASHFLOW_L{i}'
        df[col] = df[col].fillna('').astype(str).str.strip()
    
    # Build tree - INCLUDING rows where values are blank
    tree = {'options': set(), 'children': {}}
    
    for _, row in df.iterrows():
        current_node = tree
        
        for level in range(1, 4):
            value = row[f'CASHFLOW_L{level}']
            
            # Add to options (including blank)
            current_node['options'].add(value)
            
            # Create child node if doesn't exist
            if value not in current_node['children']:
                current_node['children'][value] = {
                    'options': set(),
                    'children': {}
                }
            
            # Move to child node
            current_node = current_node['children'][value]
    
    # Convert sets to sorted lists
    def convert_and_sort(node):
        options_list = list(node['options'])
        
        # Sort with blank first
        if '' in options_list:
            options_list.remove('')
            sorted_options = [''] + sorted([opt for opt in options_list if opt])
        else:
            sorted_options = sorted([opt for opt in options_list if opt])
        
        node['options'] = sorted_options
        
        for child in node['children'].values():
            convert_and_sort(child)
    
    convert_and_sort(tree)
    
    return tree


# ============================================
# CACHED LOADERS
# ============================================

@st.cache_data(show_spinner="Building metric cascade...", ttl=3600)
def _get_metric_cascade(_metric_hierarchy_df, company, version):
    """Build and cache metric cascade tree"""
    return build_metric_tree_from_df(_metric_hierarchy_df)


@st.cache_data(show_spinner="Building cashflow cascade...", ttl=3600)
def _get_cashflow_cascade(_cashflow_hierarchy_df, version):
    """Build and cache cashflow cascade tree"""
    return build_cashflow_tree_from_df(_cashflow_hierarchy_df)


# ============================================
# PUBLIC ACCESS FUNCTIONS
# ============================================

def load_metric_cascade(company, metric_hierarchy_table, metric_hierarchy_df):
    """Load metric cascade with automatic cache invalidation"""
    version = get_hierarchy_version(company, metric_hierarchy_table)
    tree = _get_metric_cascade(metric_hierarchy_df, company, version)
    return tree


def load_cashflow_cascade(cashflow_hierarchy_df):
    """Load cashflow cascade with automatic cache invalidation"""
    version = get_hierarchy_version('SHARED', 'CASHFLOW_HIERARCHY')
    tree = _get_cashflow_cascade(cashflow_hierarchy_df, version)
    return tree


# ============================================
# TREE NAVIGATION
# ============================================

def get_options_from_tree(tree, *parent_values):
    """
    Navigate tree to get options for current level
    
    Args:
        tree: Pre-built tree structure
        *parent_values: Values from parent levels
    
    Returns:
        list: Available options (including blank if present)
    """
    current_node = tree
    
    # Navigate down the tree following parent path
    for parent_value in parent_values:
        # Normalize parent value (None or NaN becomes empty string)
        parent_key = str(parent_value).strip() if parent_value is not None and pd.notna(parent_value) else ''
        
        # If parent not in tree, return empty options
        if parent_key not in current_node.get('children', {}):
            return []
        
        # Move to child node
        current_node = current_node['children'][parent_key]
    
    # Return options at this level (includes blank if it exists)
    return current_node.get('options', [])


# # ============================================
# # DEBUG HELPERS
# # ============================================

def get_cascade_stats(tree, level=0):
    """Get statistics about the cascade tree"""
    stats = {
        'total_nodes': 0,
        'max_depth': 0,
        'total_options': len(tree.get('options', [])),
        'has_blank': '' in tree.get('options', [])
    }
    
    if tree.get('children'):
        stats['total_nodes'] = len(tree['children'])
        
        for child in tree['children'].values():
            child_stats = get_cascade_stats(child, level + 1)
            stats['total_nodes'] += child_stats['total_nodes']
            stats['max_depth'] = max(stats['max_depth'], child_stats['max_depth'])
    
    stats['max_depth'] = max(stats['max_depth'], level)
    
    return stats


# def print_tree_structure(tree, indent=0, max_depth=2):
#     """
#     Debug helper to print tree structure
    
#     Args:
#         tree: Tree to print
#         indent: Current indentation level
#         max_depth: Maximum depth to print
#     """
#     if indent > max_depth:
#         return
    
#     prefix = "  " * indent
#     options = tree.get('options', [])
    
#     print(f"{prefix}Options ({len(options)}): {options[:10]}" + (" ..." if len(options) > 10 else ""))
    
#     if tree.get('children') and indent < max_depth:
#         print(f"{prefix}Children ({len(tree['children'])}):")
#         for key, child in list(tree['children'].items())[:5]:
#             display_key = '[blank]' if key == '' else key
#             print(f"{prefix}  → {display_key}")
#             print_tree_structure(child, indent + 2, max_depth)