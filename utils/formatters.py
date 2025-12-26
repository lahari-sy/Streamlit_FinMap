# utils/formatters.py
import pandas as pd

def format_value_display(value):
    """Format value for display in selectbox"""
    return value if value else "[NULL/Blank]"


def get_row_key(row_idx, *pk_values):
    """Generate unique key for each row"""
    return f"{row_idx}_{'_'.join(str(v) for v in pk_values)}"


def convert_df_to_csv(df):
    """Convert dataframe to CSV"""
    return df.to_csv(index=False)


def create_csv_template(primary_keys, mapping_cols):
    """Create CSV template for COA"""
    template_df = pd.DataFrame(columns=primary_keys + mapping_cols)
    return template_df.to_csv(index=False)


def create_adj_csv_template(adj_primary_keys):
    """Create adjustments CSV template"""
    template_df = pd.DataFrame(columns=adj_primary_keys + ['ADJ_TYPE', 'MONTH', 'YEAR', 'AMOUNT'])
    return template_df.to_csv(index=False)


def create_zeus_adj_csv_template(adj_primary_keys, metric_columns):
    """Create Zeus adjustments CSV template"""
    template_df = pd.DataFrame(columns=adj_primary_keys + list(metric_columns) + ['ADJ_TYPE', 'MONTH', 'YEAR', 'AMOUNT'])
    return template_df.to_csv(index=False)


def get_edit_key(primary_keys_dict, primary_keys):
    """Create a unique key for a row based on primary keys"""
    return tuple(primary_keys_dict[pk] for pk in primary_keys)


def build_preview_table_html(preview_df):
    """Build HTML table for preview with styling"""
    html_table = '<div style="overflow-x:auto; max-height:500px;"><table class="preview-table">'
    html_table += '<thead><tr>'
    for col in preview_df.columns:
        html_table += f'<th>{col}</th>'
    html_table += '</tr></thead><tbody>'
    
    for idx, row in preview_df.iterrows():
        html_table += '<tr>'
        for col in preview_df.columns:
            cell_value = str(row[col])
            if '<div style=' in cell_value:
                html_table += f'<td class="changed-cell">{cell_value}</td>'
            else:
                html_table += f'<td>{cell_value}</td>'
        html_table += '</tr>'
    
    html_table += '</tbody></table></div>'
    return html_table


def format_change_display(before_val, after_val):
    """Format before/after values for preview"""
    return f"<div style='text-align:center;'><div style='color:#ffebee;text-decoration:line-through;'>{before_val}</div><hr style='margin:2px 0; border:1px solid #666;'/><div style='color:#d4edda;font-weight:bold;'>{after_val}</div></div>"


PREVIEW_TABLE_CSS = """
<style>
.preview-table {
    font-size: 0.85rem;
    border-collapse: collapse;
    width: 100%;
}
.preview-table th {
    padding: 8px;
    border: 1px solid #ddd;
    font-weight: bold;
}
.preview-table td {
    padding: 8px;
    border: 1px solid #ddd;
    vertical-align: middle;
}
.preview-table tr:hover {
    background-color: #515151;
}
.changed-cell {
    background-color: #404040;
}
</style>
"""