"""
Job Orchestration Dashboard
Manages dbt jobs and Power BI refreshes per company
"""

import streamlit as st
import requests
import msal
import json
import time
from datetime import datetime
from snowflake.snowpark.context import get_active_session
from config.user_config import USER_CONFIG, JOB_CONFIG,API_CONFIG

# ============================================
# PAGE CONFIG
# ============================================

st.set_page_config(page_title="Job Orchestration", page_icon="🔄", layout="wide")

# ============================================
# SECRETS & SESSION
# ============================================

# @st.cache_data(ttl=3600)
# def get_secrets():
#     """Load all secrets from Snowflake"""
#     try:
#         session = get_active_session()
        
#         # Get general config (Power BI + dbt auth)
#         general_result = '''{
#                   "client_id": "e5df0a0c-107c-4bd2-8229-1a85d2860d4b",
#                   "client_secret": "VeJ8Q~rSBlXl4zorNuf0mwz9iXE6YAPGuyQr5bXQ",
#                   "tenant_id": "e576405c-f3a1-4c1d-b1c2-7a7207f47721",
#                   "account_id": "70471823488007",
#                   "dbt_api_token": "dbtu_n9MVfyEDe-wC3TXGQ3DwxyKkhntJPnc_5QH3WJVE030QHL3Hv0"
#                   }'''
        
#         # Get job config (company-specific IDs)
#         job_result = '''{
#               "wagway": {
#                 "workspace_id": "ff2167d0-61dd-4252-b5ff-97ba1cf0977c",
#                 "dataset_id": "f4f1277b-3433-4beb-b730-0cf5da20e7f5",
#                 "dbt_coa_job_id": "70471823539587",
#                 "dbt_adj_job_id": "70471823539587",
#                 "dbt_coa_adj_id": "70471823539587"
#               },
#               "playfly": {
#                 "workspace_id": "workspace-id-2",
#                 "dataset_id": "dataset-id-2",
#                 "dbt_coa_job_id": "12348",
#                 "dbt_adj_job_id": "12349",
#                 "dbt_coa_adj_id": "12350"}}'''
                    
#         if general_result and job_result:
#             return {
#                 'general': json.loads(general_result[0][0]),
#                 'jobs': json.loads(job_result[0][0])
#             }
#         return None
#     except Exception as e:
#         st.error(f"Error loading secrets: {e}")
#         return None

# secrets = get_secrets()

# if not secrets:
#     st.error("❌ Failed to load configuration")
#     st.stop()

general_config = API_CONFIG
# job_config = secrets['jobs']

# ============================================
# AUTHENTICATION
# ============================================

@st.cache_data(ttl=3000)
def get_powerbi_token():
    """Get Power BI access token"""
    try:
        app = msal.ConfidentialClientApplication(
            client_id=general_config['client_id'],
            authority=f"https://login.microsoftonline.com/{general_config['tenant_id']}",
            client_credential=general_config['client_secret']
        )
        
        result = app.acquire_token_for_client(
            scopes=["https://analysis.windows.net/powerbi/api/.default"]
        )
        
        return result.get("access_token")
    except Exception as e:
        st.error(f"Power BI auth error: {e}")
        return None

# ============================================
# DBT FUNCTIONS
# ============================================

def trigger_dbt_job(job_id, cause="Triggered from Streamlit"):
    """Trigger a dbt Cloud job"""
    account_id = general_config['account_id']
    api_token = general_config['dbt_api_token']
    host = general_config['host']
    url = f"https://{host}/api/v2/accounts/{account_id}/jobs/{job_id}/run/"
    headers = {
        "Authorization": f"Token {api_token}",
        "Content-Type": "application/json"
    }
    payload = {"cause": cause}
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            run_data = data.get('data', {})
            
            return {
                'success': True,
                'run_id': run_data.get('id'),
                'message': 'dbt job triggered successfully'
            }
        else:
            return {
                'success': False,
                'error': f"HTTP {response.status_code}: {response.text[:200]}"
            }
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }


def get_dbt_run_status(run_id):
    """Get dbt run status"""
    account_id = general_config['account_id']
    host = general_config['host']
    api_token = general_config['dbt_api_token']
    
    url = f"https://{host}/api/v2/accounts/{account_id}/runs/{run_id}/"
    headers = {"Authorization": f"Token {api_token}"}
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            run_data = data.get('data', {})
            
            status_code = run_data.get('status')
            
            # Map status codes
            status_map = {
                1: "Queued",
                2: "Starting",
                3: "Running",
                10: "Success",
                20: "Error",
                30: "Cancelled"
            }
            
            status_text = status_map.get(status_code, "Unknown")
            
            return {
                'success': True,
                'status': status_text,
                'status_code': status_code,
                'is_complete': status_code in [10, 20, 30],
                'is_success': status_code == 10,
                'started_at': run_data.get('created_at'),
                'finished_at': run_data.get('finished_at'),
                'duration': run_data.get('duration')
            }
        else:
            return {
                'success': False,
                'error': f"HTTP {response.status_code}"
            }
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }

# ============================================
# POWER BI FUNCTIONS
# ============================================

def refresh_powerbi_dataset(workspace_id, dataset_id):
    """Trigger Power BI dataset refresh"""
    token = get_powerbi_token()
    
    if not token:
        return {
            'success': False,
            'error': 'Failed to get Power BI token'
        }
    
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {"notifyOption": "NoNotification"}
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 202:
            return {
                'success': True,
                'request_id': response.headers.get('RequestId', 'N/A'),
                'message': 'Power BI refresh triggered'
            }
        else:
            return {
                'success': False,
                'error': f"HTTP {response.status_code}: {response.text[:200]}"
            }
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }


def get_powerbi_refresh_status(workspace_id, dataset_id):
    """Get Power BI refresh status"""
    token = get_powerbi_token()
    
    if not token:
        return {
            'success': False,
            'error': 'Failed to get Power BI token'
        }
    
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top=1"
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            refreshes = data.get('value', [])
            
            if refreshes:
                latest = refreshes[0]
                status = latest.get('status', 'Unknown')
                
                return {
                    'success': True,
                    'status': status,
                    'is_complete': status in ['Completed', 'Failed', 'Disabled'],
                    'is_success': status == 'Completed',
                    'start_time': latest.get('startTime'),
                    'end_time': latest.get('endTime')
                }
            else:
                return {
                    'success': True,
                    'status': 'No History',
                    'is_complete': True,
                    'is_success': False
                }
        else:
            return {
                'success': False,
                'error': f"HTTP {response.status_code}"
            }
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }

# ============================================
# COMBINED PIPELINE FUNCTION
# ============================================

def run_full_pipeline(job_id, workspace_id, dataset_id, pipeline_name):
    """
    Run dbt job and then Power BI refresh
    
    Returns:
        dict: Status of both steps
    """
    result = {
        'dbt': {'success': False, 'message': ''},
        'powerbi': {'success': False, 'message': ''}
    }
    
    # Step 1: Trigger dbt job
    dbt_result = trigger_dbt_job(job_id, f"{pipeline_name} - Full Pipeline")
    
    if not dbt_result['success']:
        result['dbt'] = {
            'success': False,
            'message': f"Failed to trigger: {dbt_result.get('error')}"
        }
        return result
    
    run_id = dbt_result['run_id']
    result['dbt'] = {
        'success': True,
        'message': f"Job triggered (Run ID: {run_id})",
        'run_id': run_id,
        'status': 'Running'
    }
    
    # Wait for dbt job to complete
    max_wait = 600  # 10 minutes
    elapsed = 0
    poll_interval = 10
    
    while elapsed < max_wait:
        time.sleep(poll_interval)
        elapsed += poll_interval
        
        status_result = get_dbt_run_status(run_id)
        
        if status_result['success'] and status_result['is_complete']:
            if status_result['is_success']:
                result['dbt']['status'] = 'Success'
                result['dbt']['message'] = f"dbt job completed successfully"
                break
            else:
                result['dbt']['status'] = 'Failed'
                result['dbt']['message'] = f"dbt job failed with status: {status_result['status']}"
                return result  # Don't trigger Power BI if dbt failed
    
    if elapsed >= max_wait:
        result['dbt']['status'] = 'Timeout'
        result['dbt']['message'] = 'dbt job did not complete within 10 minutes'
        return result
    
    # Step 2: Trigger Power BI refresh (only if dbt succeeded)
    pbi_result = refresh_powerbi_dataset(workspace_id, dataset_id)
    
    if pbi_result['success']:
        result['powerbi'] = {
            'success': True,
            'message': f"Power BI refresh triggered (Request ID: {pbi_result['request_id']})",
            'status': 'Triggered'
        }
    else:
        result['powerbi'] = {
            'success': False,
            'message': f"Power BI refresh failed: {pbi_result.get('error')}"
        }
    
    return result

# ============================================
# UI COMPONENTS
# ============================================

def render_pipeline_card(title, job_id, workspace_id, dataset_id, key_prefix):
    """Render a pipeline card with sequential status tracking"""
    
    # Initialize session state
    if f"{key_prefix}_dbt_status" not in st.session_state:
        st.session_state[f"{key_prefix}_dbt_status"] = None
    if f"{key_prefix}_pbi_status" not in st.session_state:
        st.session_state[f"{key_prefix}_pbi_status"] = None
    if f"{key_prefix}_run_id" not in st.session_state:
        st.session_state[f"{key_prefix}_run_id"] = None
    if f"{key_prefix}_last_updated" not in st.session_state:
        st.session_state[f"{key_prefix}_last_updated"] = None
    
    # Card container
    with st.container(border=True):
        # Header
        st.subheader(title)
        
        # Sequential status display (vertical)
        st.markdown("**Pipeline Status**")
        
        # Step 1: dbt Job
        dbt_status = st.session_state[f"{key_prefix}_dbt_status"]
        
        if dbt_status is None:
            st.markdown("**1. dbt Job:** Not Started")
        elif dbt_status == "Running":
            st.markdown("**1. dbt Job:** 🔄 Running...")
        elif dbt_status == "Success":
            st.markdown("**1. dbt Job:** ✓ Completed")
        elif dbt_status == "Failed":
            st.markdown("**1. dbt Job:** ✗ Failed")
        elif dbt_status == "Queued":
            st.markdown("**1. dbt Job:** ⏳ Queued")
        else:
            st.markdown(f"**1. dbt Job:** {dbt_status}")
        
        # Step 2: Power BI (only show if dbt succeeded)
        pbi_status = st.session_state[f"{key_prefix}_pbi_status"]
        
        if dbt_status != "Success":
            st.markdown("**2. Power BI Refresh:** Waiting for dbt completion")
        elif pbi_status is None:
            st.markdown("**2. Power BI Refresh:** Ready to trigger")
        elif pbi_status == "InProgress":
            st.markdown("**2. Power BI Refresh:** 🔄 In Progress...")
        elif pbi_status == "Completed":
            st.markdown("**2. Power BI Refresh:** ✓ Completed")
        elif pbi_status == "Failed":
            st.markdown("**2. Power BI Refresh:** ✗ Failed")
        elif pbi_status == "Triggered":
            st.markdown("**2. Power BI Refresh:** ⏳ Starting...")
        else:
            st.markdown(f"**2. Power BI Refresh:** {pbi_status}")
        
        # Last updated timestamp
        if st.session_state[f"{key_prefix}_last_updated"]:
            st.caption(f"Last updated: {st.session_state[f'{key_prefix}_last_updated']}")
        
        st.divider()
        
        # Action buttons
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("Run Pipeline", key=f"{key_prefix}_run", use_container_width=True, type="primary"):
                # Reset states
                st.session_state[f"{key_prefix}_dbt_status"] = "Running"
                st.session_state[f"{key_prefix}_pbi_status"] = None
                st.session_state[f"{key_prefix}_last_updated"] = datetime.now().strftime("%H:%M:%S")
                
                with st.spinner(f"Starting {title} pipeline..."):
                    result = run_full_pipeline(job_id, workspace_id, dataset_id, title)
                
                # Update dbt status
                if result['dbt']['success']:
                    st.session_state[f"{key_prefix}_dbt_status"] = result['dbt'].get('status', 'Success')
                    st.session_state[f"{key_prefix}_run_id"] = result['dbt'].get('run_id')
                    st.success(f"dbt: {result['dbt']['message']}")
                else:
                    st.session_state[f"{key_prefix}_dbt_status"] = 'Failed'
                    st.error(f"dbt: {result['dbt']['message']}")
                
                # Update Power BI status (only if dbt succeeded)
                if result['dbt'].get('status') == 'Success':
                    if result['powerbi']['success']:
                        st.session_state[f"{key_prefix}_pbi_status"] = result['powerbi'].get('status', 'Triggered')
                        st.success(f"Power BI: {result['powerbi']['message']}")
                    elif result['powerbi'].get('message'):
                        st.session_state[f"{key_prefix}_pbi_status"] = 'Failed'
                        st.error(f"Power BI: {result['powerbi']['message']}")
                
                st.session_state[f"{key_prefix}_last_updated"] = datetime.now().strftime("%H:%M:%S")
                st.rerun()
        
        with col2:
            if st.button("Check Status", key=f"{key_prefix}_status", use_container_width=True):
                updated = False
                
                # Check dbt status
                run_id = st.session_state.get(f"{key_prefix}_run_id")
                if run_id:
                    dbt_result = get_dbt_run_status(run_id)
                    if dbt_result['success']:
                        st.session_state[f"{key_prefix}_dbt_status"] = dbt_result['status']
                        st.info(f"dbt: {dbt_result['status']}")
                        updated = True
                
                # Check Power BI status (only if dbt succeeded)
                if st.session_state[f"{key_prefix}_dbt_status"] == "Success":
                    pbi_result = get_powerbi_refresh_status(workspace_id, dataset_id)
                    if pbi_result['success']:
                        st.session_state[f"{key_prefix}_pbi_status"] = pbi_result['status']
                        st.info(f"Power BI: {pbi_result['status']}")
                        updated = True
                
                if updated:
                    st.session_state[f"{key_prefix}_last_updated"] = datetime.now().strftime("%H:%M:%S")
                    st.rerun()
                else:
                    st.warning("No active jobs to check")
        
        with col3:
            if st.button("Reset", key=f"{key_prefix}_reset", use_container_width=True):
                st.session_state[f"{key_prefix}_dbt_status"] = None
                st.session_state[f"{key_prefix}_pbi_status"] = None
                st.session_state[f"{key_prefix}_run_id"] = None
                st.session_state[f"{key_prefix}_last_updated"] = None
                st.rerun()

# ============================================
# MAIN PAGE
# ============================================

st.title("🔄 Job Orchestration Dashboard")
st.caption("Manage dbt jobs and Power BI refreshes")

company = st.session_state.get('selected_company', 'Access Holdings')
company_jobs = JOB_CONFIG.get(company, {})

if not company :
    st.error(f"❌ Job configuration not found for {company}")
    st.stop()

# Auto-refresh toggle
col_header1, col_header2 = st.columns([4, 1])

with col_header1:
    st.markdown(f"## {selected_company} Pipelines")

with col_header2:
    auto_refresh = st.toggle("Auto-refresh (30s)", key="auto_refresh")

st.divider()

# Pipeline cards in vertical layout
render_pipeline_card(
    title="COA Mapping Pipeline",
    job_id=company_jobs['dbt_coa_job_id'],
    workspace_id=company_jobs['workspace_id'],
    dataset_id=company_jobs['dataset_id'],
    key_prefix=f"{company}_coa"
)

render_pipeline_card(
    title="Adjustments Pipeline",
    job_id=company_jobs['dbt_adj_job_id'],
    workspace_id=company_jobs['workspace_id'],
    dataset_id=company_jobs['dataset_id'],
    key_prefix=f"{company}_adj"
)

render_pipeline_card(
    title="Combined Pipeline",
    job_id=company_jobs['dbt_coa_adj_id'],
    workspace_id=company_jobs['workspace_id'],
    dataset_id=company_jobs['dataset_id'],
    key_prefix=f"{company}_combined"
)

# Quick actions
st.divider()
st.subheader("Quick Actions")

col_action1, col_action2, col_action3 = st.columns(3)

with col_action1:
    if st.button("Check All Status", use_container_width=True):
        for pipeline in ['coa', 'adj', 'combined']:
            key_prefix = f"{company}_{pipeline}"
            
            # Check dbt
            run_id = st.session_state.get(f"{key_prefix}_run_id")
            if run_id:
                dbt_result = get_dbt_run_status(run_id)
                if dbt_result['success']:
                    st.session_state[f"{key_prefix}_dbt_status"] = dbt_result['status']
            
            # Check Power BI (only if dbt succeeded)
            if st.session_state.get(f"{key_prefix}_dbt_status") == "Success":
                pbi_result = get_powerbi_refresh_status(company_jobs['workspace_id'], company_jobs['dataset_id'])
                if pbi_result['success']:
                    st.session_state[f"{key_prefix}_pbi_status"] = pbi_result['status']
            
            st.session_state[f"{key_prefix}_last_updated"] = datetime.now().strftime("%H:%M:%S")
        
        st.success("Status updated for all pipelines")
        st.rerun()

with col_action2:
    if st.button("Reset All", use_container_width=True):
        for pipeline in ['coa', 'adj', 'combined']:
            key_prefix = f"{company}_{pipeline}"
            st.session_state[f"{key_prefix}_dbt_status"] = None
            st.session_state[f"{key_prefix}_pbi_status"] = None
            st.session_state[f"{key_prefix}_run_id"] = None
            st.session_state[f"{key_prefix}_last_updated"] = None
        st.success("All pipelines reset")
        st.rerun()

with col_action3:
    if st.button("Refresh Page", use_container_width=True):
        st.rerun()

# Auto-refresh implementation
if auto_refresh:
    st.info("Auto-refresh enabled - updating every 30 seconds")
    time.sleep(30)
    st.rerun()


