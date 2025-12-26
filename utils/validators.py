# utils/validators.py
import pandas as pd
import re


def normalize_value(value):
    """
    Normalize a value to handle NaN, None, and empty strings consistently
    
    Args:
        value: Any value to normalize
    
    Returns:
        str: Empty string for NaN/None/empty, otherwise stripped string
    """
    if pd.isna(value) or value is None:
        return ''
    
    str_val = str(value).strip()
    
    # Handle string 'nan', 'None', 'null', etc.
    if str_val.lower() in ['nan', 'none', 'null']:
        return ''
    
    return str_val


def normalize_csv_data(csv_df, primary_keys, mapping_cols):
    """
    Normalize CSV data - same approach as load_actual_data
    
    1. Cast primary keys to int (nulls become 0)
    2. Normalize all mapping columns (nulls become empty string)
    
    Args:
        csv_df: CSV dataframe
        primary_keys: List of primary key columns
        mapping_cols: List of mapping columns to normalize
    
    Returns:
        DataFrame: Normalized CSV data
    """
    df = csv_df.copy()
    
    # ============================================
    # NORMALIZE PRIMARY KEYS (cast to int, null→0)
    # ============================================
    for pk in primary_keys:
        if pk in df.columns:
            df[pk] = pd.to_numeric(df[pk], errors='coerce').fillna(0).astype(int)
    
    # ============================================
    # NORMALIZE MAPPING COLUMNS (null→empty string)
    # ============================================
    for col in mapping_cols:
        if col in df.columns:
            df[col] = df[col].apply(normalize_value)
    
    return df




def validate_amount(amount_str):
    """
    Validate that amount is numeric
    
    Args:
        amount_str: Amount string to validate
    
    Returns:
        bool: True if valid numeric value
    """
    amount_str = normalize_value(amount_str)
    if not amount_str:
        return False
    
    try:
        float(amount_str.replace(',', ''))
        return True
    except:
        return False


def validate_hierarchy(row, metric_df, cashflow_df, debt_options, is_dm):
    """
    Validate a single row's hierarchy mappings (for UI inline edits)
    
    Args:
        row: Row data (dict or Series) - ALREADY NORMALIZED
        metric_df: Metric hierarchy reference dataframe
        cashflow_df: Cashflow hierarchy reference dataframe
        debt_options: List of valid debt mapping options
        is_dm: Whether debt mapping validation is enabled
    
    Returns:
        list: List of error messages (empty if valid)
    """
    errors = []
    
    # ============================================
    # VALIDATE METRIC HIERARCHY
    # ============================================
    
    # Build metric path from row
    metric_path = '|'.join([
        normalize_value(row.get(f'METRIC_L{i}', ''))
        for i in range(1, 7)
    ])
    
    # Build valid metric paths from hierarchy
    metric_df_temp = metric_df.copy()
    metric_df_temp['_PATH_KEY'] = metric_df_temp.apply(
        lambda r: '|'.join([
            normalize_value(r.get(f'METRIC_L{i}', ''))
            for i in range(1, 7)
        ]), axis=1
    )
    valid_metric_paths = set(metric_df_temp['_PATH_KEY'])
    
    if metric_path not in valid_metric_paths:
        errors.append(f"Invalid metric hierarchy: {metric_path.replace('|', ' → ')}")
    
    # ============================================
    # VALIDATE IS_BS
    # ============================================
    
    is_bs = normalize_value(row.get('IS_BS', ''))
    if is_bs not in ['', 'IS', 'BS']:
        errors.append(f"IS_BS must be blank, 'IS', or 'BS', got '{is_bs}'")
    
    # ============================================
    # VALIDATE CASHFLOW (only for BS)
    # ============================================
    
    if is_bs == 'BS':
        cf_path = '|'.join([
            normalize_value(row.get(f'CASHFLOW_L{i}', ''))
            for i in range(1, 4)
        ])
        
        cashflow_df_temp = cashflow_df.copy()
        cashflow_df_temp['_PATH_KEY'] = cashflow_df_temp.apply(
            lambda r: '|'.join([
                normalize_value(r.get(f'CASHFLOW_L{i}', ''))
                for i in range(1, 4)
            ]), axis=1
        )
        valid_cf_paths = set(cashflow_df_temp['_PATH_KEY'])
        
        if cf_path not in valid_cf_paths:
            errors.append(f"Invalid cashflow hierarchy: {cf_path.replace('|', ' → ')}")
    
    elif is_bs == 'IS':
        # IS should have blank cashflow columns
        for i in range(1, 4):
            cf_val = normalize_value(row.get(f'CASHFLOW_L{i}', ''))
            if cf_val != '':
                errors.append(f"CASHFLOW_L{i} should be blank for IS_BS='IS'")
                break
    
    # ============================================
    # VALIDATE DEBT MAPPING
    # ============================================
    
    if is_dm:
        l1 = normalize_value(row.get('METRIC_L1', ''))
        debt_val = normalize_value(row.get('DEBT_MAPPING', ''))
        
        if l1 in ['Current Liabilities', 'Non-Current Liabilities']:
            if debt_val and debt_val not in debt_options:
                errors.append(f"Invalid DEBT_MAPPING: '{debt_val}' (not in valid options)")
        else:
            if debt_val != '':
                errors.append(f"DEBT_MAPPING should be blank when METRIC_L1 is not a liability")
    
    return errors


def validate_csv_hierarchy(csv_df, metric_df, cashflow_df, debt_options, is_dm, primary_keys):
    """
    Validate CSV hierarchy using composite key matching (FAST & SIMPLE)
    
    This approach:
    1. Normalizes CSV data (PKs to int, mapping cols to clean strings)
    2. Creates composite path keys (L1|L2|L3|L4|L5|L6) in hierarchy tables
    3. Creates same keys in CSV rows
    4. Checks if CSV keys exist in valid hierarchy keys (set lookup)
    
    Args:
        csv_df: CSV dataframe to validate
        metric_df: Metric hierarchy reference dataframe
        cashflow_df: Cashflow hierarchy reference dataframe
        debt_options: List of valid debt mapping options
        is_dm: Whether debt mapping validation is enabled
        primary_keys: List of primary key column names
    
    Returns:
        tuple: (errors_list, valid_row_indices)
    """
    errors = []
    
    # Define mapping columns to normalize
    mapping_cols = [
        'METRIC_L1', 'METRIC_L2', 'METRIC_L3', 'METRIC_L4', 'METRIC_L5', 'METRIC_L6',
        'IS_BS',
        'CASHFLOW_L1', 'CASHFLOW_L2', 'CASHFLOW_L3',
        'DEBT_MAPPING'
    ]
    
    # ============================================
    # STEP 0: NORMALIZE CSV DATA (like load_actual_data)
    # ============================================
    
    csv_df = normalize_csv_data(csv_df, primary_keys, mapping_cols)
    
    # Also normalize hierarchy dataframes
    metric_df = metric_df.copy()
    cashflow_df = cashflow_df.copy()
    
    for i in range(1, 7):
        col = f'METRIC_L{i}'
        if col in metric_df.columns:
            metric_df[col] = metric_df[col].apply(normalize_value)
    
    for i in range(1, 4):
        col = f'CASHFLOW_L{i}'
        if col in cashflow_df.columns:
            cashflow_df[col] = cashflow_df[col].apply(normalize_value)
    
    # ============================================
    # STEP 1: BUILD METRIC PATH KEYS
    # ============================================
    
    # Create composite key in metric hierarchy
    metric_df['_METRIC_PATH'] = (
        metric_df['METRIC_L1'] + '|' +
        metric_df['METRIC_L2'] + '|' +
        metric_df['METRIC_L3'] + '|' +
        metric_df['METRIC_L4'] + '|' +
        metric_df['METRIC_L5'] + '|' +
        metric_df['METRIC_L6']
    )
    
    # Get set of valid metric paths
    valid_metric_paths = set(metric_df['_METRIC_PATH'])
    
    # Create same composite key in CSV
    csv_df['_METRIC_PATH'] = (
        csv_df['METRIC_L1'] + '|' +
        csv_df['METRIC_L2'] + '|' +
        csv_df['METRIC_L3'] + '|' +
        csv_df['METRIC_L4'] + '|' +
        csv_df['METRIC_L5'] + '|' +
        csv_df['METRIC_L6']
    )
    
    # Check which CSV rows have invalid metric paths
    csv_df['_METRIC_VALID'] = csv_df['_METRIC_PATH'].isin(valid_metric_paths)
    
    # ============================================
    # STEP 2: BUILD CASHFLOW PATH KEYS
    # ============================================
    
    # Create composite key in cashflow hierarchy
    cashflow_df['_CF_PATH'] = (
        cashflow_df['CASHFLOW_L1'] + '|' +
        cashflow_df['CASHFLOW_L2'] + '|' +
        cashflow_df['CASHFLOW_L3']
    )
    
    # Get set of valid cashflow paths
    valid_cf_paths = set(cashflow_df['_CF_PATH'])
    
    # Create same composite key in CSV
    csv_df['_CF_PATH'] = (
        csv_df['CASHFLOW_L1'] + '|' +
        csv_df['CASHFLOW_L2'] + '|' +
        csv_df['CASHFLOW_L3']
    )
    
    # Check cashflow validity (only for IS_BS = 'BS')
    csv_df['_CF_VALID'] = True  # Default to valid
    bs_mask = csv_df['IS_BS'] == 'BS'
    csv_df.loc[bs_mask, '_CF_VALID'] = csv_df.loc[bs_mask, '_CF_PATH'].isin(valid_cf_paths)
    
    # ============================================
    # STEP 3: VALIDATE EACH ROW
    # ============================================
    
    valid_rows = []
    
    for idx, row in csv_df.iterrows():
        row_errors = []
        
        # Check metric hierarchy
        if not row['_METRIC_VALID']:
            metric_display = row['_METRIC_PATH'].replace('|', ' → ').strip(' → ')
            if not metric_display:
                metric_display = "[ALL BLANK]"
            row_errors.append(f"Invalid metric hierarchy: {metric_display}")
        
        # Check IS_BS value
        is_bs = row['IS_BS']
        if is_bs not in ['', 'IS', 'BS']:
            row_errors.append(f"IS_BS must be blank, 'IS', or 'BS', got '{is_bs}'")
        
        # Check cashflow (only for BS)
        if is_bs == 'BS':
            if not row['_CF_VALID']:
                cf_display = row['_CF_PATH'].replace('|', ' → ').strip(' → ')
                if not cf_display:
                    cf_display = "[ALL BLANK]"
                row_errors.append(f"Invalid cashflow hierarchy: {cf_display}")
        elif is_bs == 'IS':
            # IS should have blank cashflow (all blank = ||)
            if row['_CF_PATH'] != '||':
                row_errors.append(f"Cashflow columns should be blank for IS_BS='IS'")
        
        # Check debt mapping
        if is_dm:
            l1 = row['METRIC_L1']
            debt_val = row['DEBT_MAPPING']
            
            if l1 in ['Current Liabilities', 'Non-Current Liabilities']:
                if debt_val and debt_val not in debt_options:
                    row_errors.append(f"Invalid DEBT_MAPPING: '{debt_val}' (not in valid options)")
            else:
                if debt_val != '':
                    row_errors.append(f"DEBT_MAPPING should be blank when METRIC_L1 is not a liability")
        
        # Collect results
        if row_errors:
            # Excel row number = idx + 2 (header is row 1, data starts at row 2)
            errors.append(f"**Row {idx + 2}:** {' | '.join(row_errors)}")
        else:
            valid_rows.append(idx)
    
    return errors, valid_rows


def validate_adjustment_row(row, months, years, adj_types, adj_primary_keys):
    """
    Validate a single adjustment row
    
    Args:
        row: Row data (dict or Series)
        months: List of valid month values
        years: List of valid year values
        adj_types: List of valid adjustment types
        adj_primary_keys: List of primary key column names for adjustments
    
    Returns:
        list: List of error messages (empty if valid)
    """
    errors = []
    
    # Check required fields (with proper NaN handling)
    adj_type = normalize_value(row.get('ADJ_TYPE', ''))
    month = normalize_value(row.get('MONTH', ''))
    year = normalize_value(row.get('YEAR', ''))
    amount = row.get('AMOUNT', '')
    
    # Validate ADJ_TYPE
    if not adj_type:
        errors.append("ADJ_TYPE is required")
    elif adj_type not in adj_types:
        errors.append(f"Invalid ADJ_TYPE: '{adj_type}'. Must be one of: {adj_types}")
    
    # Validate MONTH
    if not month:
        errors.append("MONTH is required")
    elif month not in months:
        errors.append(f"Invalid MONTH: '{month}'. Must be one of: {months}")
    
    # Validate YEAR
    if not year:
        errors.append("YEAR is required")
    elif year not in years:
        errors.append(f"Invalid YEAR: '{year}'. Must be one of: {years}")
    
    # Validate AMOUNT
    if not validate_amount(amount):
        errors.append(f"Invalid AMOUNT: '{amount}'. Must be a valid number")
    
    # Validate Lender/Pro-Forma adjustments (all primary keys should be 0 or blank)
    if adj_type in ['Lender Adjustment', 'Pro-Forma Adjustment']:
        for pk in adj_primary_keys:
            pk_val = normalize_value(row.get(pk, ''))
            if pk_val not in ['', '0']:
                errors.append(f"For {adj_type}, {pk} must be 0 or blank, got '{pk_val}'")
    
    return errors