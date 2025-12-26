# components/adjustments_helper.py
import pandas as pd
import streamlit as st
from datetime import datetime
from utils.data_loader import get_current_user
from config.settings import MONTHS


def convert_month_year_to_period(month, year):
    """
    Convert MONTH and YEAR to PERIOD format
    
    Args:
        month: Month number (1-12)
        year: Year (e.g., 2024)
        months_list: List of month names (e.g., ["Jan", "Feb", ...])
    
    Returns:
        str: Period in format "Jan-24"
    """
    try:
        month_num = int(month)
        year_num = int(year)
        
        period = f"{MONTHS[month_num]}-{str(year_num)[2:]}"
        return period
    
    except Exception as e:
        raise ValueError(f"Error converting month/year to period: {e}")


def normalize_adjustment_csv(csv_df, primary_keys):
    """
    Normalize adjustment CSV data
    
    Args:
        csv_df: Raw CSV dataframe
        primary_keys: List of primary key column names
    
    Returns:
        Normalized dataframe
    """
    df = csv_df.copy()
    
    # Strip whitespace from string columns
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    
    # Normalize primary keys to int
    for pk in primary_keys:
        if pk in df.columns:
            df[pk] = df[pk].replace(['', ' ', None], 0)
            df[pk] = pd.to_numeric(df[pk], errors='coerce').fillna(0).astype(int)
    
    # Normalize MONTH and YEAR
    for col in ['MONTH', 'YEAR']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Normalize AMOUNT
    if 'AMOUNT' in df.columns:
        df['AMOUNT'] = pd.to_numeric(df['AMOUNT'], errors='coerce').fillna(0)
    
    return df


def validate_adjustment_csv(csv_df, primary_keys, years_list, adj_type_options):
    """
    Validate adjustment CSV data
    
    Args:
        csv_df: Normalized CSV dataframe
        primary_keys: List of primary key column names
        months_dict: Dict mapping month number to name {1: "Jan", ...}
        years_list: List of valid years
        adj_type_options: List of valid adjustment types
    
    Returns:
        Tuple: (validation_errors list, valid_row_indices list)
    """
    validation_errors = []
    valid_rows = []
    
    for idx, row in csv_df.iterrows():
        errors = []
        
        # Check required fields
        if pd.isna(row['ADJ_TYPE']) or str(row['ADJ_TYPE']).strip() == '':
            errors.append("ADJ_TYPE is required")
        
        if pd.isna(row['MONTH']) or str(row['MONTH']).strip() == '':
            errors.append("MONTH is required")
        
        if pd.isna(row['YEAR']) or str(row['YEAR']).strip() == '' :
            errors.append("YEAR is required")
        
        if pd.isna(row['AMOUNT']) or str(row['AMOUNT']).strip() == '':
            errors.append("AMOUNT is required")
        
        # Validate values if required fields are present
        if not errors:
            # Validate MONTH (1-12)
            try:
                month_num = int(row['MONTH'])
                if month_num< 1 or month_num >12:
                    errors.append(f"Invalid MONTH: {month_num} (Expected: 1-12)")
            except (ValueError, TypeError):
                errors.append(f"Invalid MONTH format: {row['MONTH']}")
            
            # Validate YEAR
            try:
                year_num = int(row['YEAR'])
                if year_num not in years_list:
                    errors.append(f"Invalid YEAR: {year_num} (Expected: {min(years_list)}-{max(years_list)})")
            except (ValueError, TypeError):
                errors.append(f"Invalid YEAR format: {row['YEAR']}")
            
            # Validate AMOUNT
            try:
                float(row['AMOUNT'])
            except (ValueError, TypeError):
                errors.append(f"Invalid AMOUNT: {row['AMOUNT']} (Must be numeric)")
            
            # Validate ADJ_TYPE
            if row['ADJ_TYPE'] not in adj_type_options:
                errors.append(f"Invalid ADJ_TYPE: {row['ADJ_TYPE']}")
            
            # Validate Lender/Pro-Forma rules
            elif row['ADJ_TYPE'] in ('Lender Adjustment', 'Pro-Forma Adjustment'):
                for pk in primary_keys:
                    if int(row[pk]) != 0:
                        errors.append(f"For {row['ADJ_TYPE']}, {pk} must be 0 (got {row[pk]})")
                        break
        
        if errors:
            pk_display = "-".join(str(row[pk]) for pk in primary_keys)
            validation_errors.append(f"**Row {idx + 2}** ({pk_display}): {' | '.join(errors)}")
        else:
            valid_rows.append(idx)
    
    return validation_errors, valid_rows


def build_coa_lookup(coa_df, primary_keys, display_columns):
    """
    Build lookup dictionaries for COA display values
    
    Args:
        coa_df: COA reference dataframe
        primary_keys: List of primary key column names
        display_columns: List of display column names
    
    Returns:
        Dict mapping primary keys to display column lookups
    """
    lookup_dicts = {}
    
    num_pks = len(primary_keys)
    num_display_cols = len(display_columns)
    
    if num_display_cols == num_pks + 1:
        # Extra display column at the end (ACCOUNT_NUMBER)
        # Build lookups for primary keys → corresponding display columns
        for i, (pk, display_col) in enumerate(zip(primary_keys, display_columns[:num_pks])):
            lookup_dicts[pk] = dict(zip(coa_df[pk], coa_df[display_col]))
        
        # Special handling for ACCOUNT_NUMBER
        # ACCOUNT_NUMBER is looked up from ACCOUNT_ID using ACCOUNT_NUMBER column
        extra_display_col = display_columns[-1]  # ACCOUNT_NUMBER
        first_pk = primary_keys[0]  # ACCOUNT_ID
        
        # Create separate lookup for ACCOUNT_NUMBER
        if extra_display_col in coa_df.columns:
            lookup_dicts[extra_display_col] = dict(zip(coa_df[first_pk], coa_df[extra_display_col]))
        else:
            # Fallback: if ACCOUNT_NUMBER not in COA, use ACCOUNT_ID as string
            lookup_dicts[extra_display_col] = dict(zip(coa_df[first_pk], coa_df[first_pk].astype(str)))
    else:
        # Equal length: direct 1-to-1 mapping
        for pk, display_col in zip(primary_keys, display_columns):
            lookup_dicts[pk] = dict(zip(coa_df[pk], coa_df[display_col]))
    
    return lookup_dicts


def prepare_upsert_dataframe(data_source, primary_keys, display_columns, coa_lookup, 
                             source_type='csv'):
    """
    Prepare dataframe for MERGE UPSERT operation
    
    Args:
        data_source: Either CSV dataframe (with MONTH/YEAR) or changes_dict (from bulk edit)
        primary_keys: List of primary key column names
        display_columns: List of display column names (may have one extra at the end)
        coa_lookup: Pre-built COA lookup dictionaries
        source_type: 'csv' or 'bulk_edit'
        months_list: List of month names (required for CSV)
    
    Returns:
        DataFrame ready for MERGE operation with all required columns
    """
    
    if source_type == 'csv':
        # CSV source: has MONTH, YEAR columns
        df = data_source.copy()
        
        # Convert MONTH/YEAR to PERIOD
        df['PERIOD'] = df.apply(
            lambda row: convert_month_year_to_period(row['MONTH'], row['YEAR']),
            axis=1
        )
        
        # Map primary keys to display columns
        num_pks = len(primary_keys)
        num_display_cols = len(display_columns)
        
        if num_display_cols == num_pks + 1:
            # Extra column at the end (ACCOUNT_NUMBER)
            # Map first N display columns to primary keys (1-to-1)
            for pk, display_col in zip(primary_keys, display_columns[:num_pks]):
                df[display_col] = df[pk].map(lambda x: coa_lookup[pk].get(int(x), 'Unknown').replace("'", "''"))
            
            # Last display column (ACCOUNT_NUMBER) has its own lookup
            extra_display_col = display_columns[-1]  # ACCOUNT_NUMBER
            first_pk = primary_keys[0]  # ACCOUNT_ID
            df[extra_display_col] = df[first_pk].map(
                lambda x: coa_lookup[extra_display_col].get(int(x), 'Unknown').replace("'", "''")
            )
        
        else:
            # Equal length: direct 1-to-1 mapping
            for pk, display_col in zip(primary_keys, display_columns):
                df[display_col] = df[pk].map(lambda x: coa_lookup[pk].get(int(x), 'Unknown').replace("'", "''"))
        
        # Select final columns
        final_cols = primary_keys + display_columns + ['PERIOD', 'ADJ_TYPE', 'AMOUNT']
        return df[final_cols]
    
    else:  # bulk_edit
        # Bulk edit source: dict with {row_key: {primary_keys, period, adj_type, new_amount}}
        rows = []
        for change in data_source.values():
            row_data = change['primary_keys'].copy()
            row_data['PERIOD'] = change['period']
            row_data['ADJ_TYPE'] = change['adj_type']
            row_data['AMOUNT'] = change['new_amount']
            
            # Map primary keys to display columns
            num_pks = len(primary_keys)
            num_display_cols = len(display_columns)
            
            if num_display_cols == num_pks + 1:
                # Extra column at the end (ACCOUNT_NUMBER)
                # Map first N display columns to primary keys (1-to-1)
                for pk, display_col in zip(primary_keys, display_columns[:num_pks]):
                    pk_value = int(row_data[pk])
                    row_data[display_col] = coa_lookup[pk].get(pk_value, 'Unknown').replace("'", "''")
                
                # Last display column (ACCOUNT_NUMBER) has its own lookup
                extra_display_col = display_columns[-1]  # ACCOUNT_NUMBER
                first_pk = primary_keys[0]  # ACCOUNT_ID
                pk_value = int(row_data[first_pk])
                row_data[extra_display_col] = coa_lookup[extra_display_col].get(pk_value, 'Unknown').replace("'", "''")
            
            else:
                # Equal length: direct 1-to-1 mapping
                for pk, display_col in zip(primary_keys, display_columns):
                    pk_value = int(row_data[pk])
                    row_data[display_col] = coa_lookup[pk].get(pk_value, 'Unknown').replace("'", "''")
            
            rows.append(row_data)
        
        return pd.DataFrame(rows)
def execute_adjustment_upsert_merge(upsert_df, table_name, primary_keys, display_columns, 
                                     session, current_user, upsert_mode=True):
    """
    Execute MERGE-based UPSERT/UPDATE for adjustments
    
    Args:
        upsert_df: DataFrame with all data to upsert (includes PKs, display cols, PERIOD, ADJ_TYPE, AMOUNT)
        table_name: Adjustments table name
        primary_keys: List of primary key column names
        display_columns: List of display column names
        session: Snowpark session
        current_user: Current user name
        upsert_mode: True for UPSERT (INSERT+UPDATE), False for UPDATE only
    
    Returns:
        Tuple: (success, insert_count, update_count, error_message)
    """
    if len(upsert_df) == 0:
        return True, 0, 0, None
    
    temp_table = None
    
    try:
        # Create temporary table
        temp_table = f"TEMP_ADJ_UPSERT_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        
        # Write data to temp table
        session.write_pandas(
            upsert_df,
            temp_table,
            auto_create_table=True,
            overwrite=True
        )
        
        # Build MERGE query
        # Match on primary keys + PERIOD + ADJ_TYPE
        match_conditions = []
        for pk in primary_keys:
            match_conditions.append(f"TARGET.{pk} = SOURCE.{pk}")
        match_conditions.append("TARGET.PERIOD = SOURCE.PERIOD")
        match_conditions.append("TARGET.ADJ_TYPE = SOURCE.ADJ_TYPE")
        
        # Get current timestamp
        current_timestamp = session.sql("SELECT CURRENT_TIMESTAMP()").collect()[0][0]
        
        # Build SET clause for UPDATE
        set_clauses = ["TARGET.AMOUNT = SOURCE.AMOUNT"]
        for display_col in display_columns:
            set_clauses.append(f"TARGET.{display_col} = SOURCE.{display_col}")
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
        if upsert_mode:
            insert_cols = primary_keys + display_columns + ['PERIOD', 'ADJ_TYPE', 'AMOUNT', 'CREATED_BY', 'CREATED_AT']
            insert_source_cols = primary_keys + display_columns + ['PERIOD', 'ADJ_TYPE', 'AMOUNT']
            
            merge_query += f"""
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(insert_cols)})
                VALUES ({', '.join([f'SOURCE.{col}' for col in insert_source_cols])}, 
                        '{current_user}', '{current_timestamp}')
            """
        
        # Execute MERGE
        result = session.sql(merge_query).collect()
        
        # Parse results
        insert_count = 0
        update_count = 0
        
        if result and len(result) > 0:
            result_dict = result[0].as_dict()
            insert_count = result_dict.get('number of rows inserted', 0)
            update_count = result_dict.get('number of rows updated', 0)
        
        # Drop temp table
        session.sql(f"DROP TABLE IF EXISTS {temp_table}").collect()
        temp_table = None
        
        return True, insert_count, update_count, None
    
    except Exception as e:
        # Drop temp table on error
        if temp_table:
            try:
                session.sql(f"DROP TABLE IF EXISTS {temp_table}").collect()
            except:
                pass
        
        return False, 0, 0, str(e)


def create_adjustment_csv_template(primary_keys):
    """
    Create CSV template for adjustments
    
    Args:
        primary_keys: List of primary key column names
    
    Returns:
        CSV string
    """
    headers = primary_keys + ['MONTH', 'YEAR', 'ADJ_TYPE', 'AMOUNT']
    template_df = pd.DataFrame(columns=headers)
    return template_df.to_csv(index=False)