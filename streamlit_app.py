#streamlit_app.py
import streamlit as st
import sys
from pathlib import Path

# Add current directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import PAGE_CONFIG, CUSTOM_CSS
from config.user_config import COMPANY_ROLES, USER_CONFIG
from utils.data_loader import get_snowpark_session, get_current_user, get_available_roles
from utils.session_manager import initialize_session_state, clear_company_state

# # Page configuration - MUST be first Streamlit command
st.set_page_config(**PAGE_CONFIG)

# Apply custom CSS
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# Get Snowflake session
session = get_snowpark_session()

# ========================================
# USER-BASED CONFIGURATION
# ========================================
current_user = get_current_user()
available_roles = get_available_roles()

# Determine which companies the user has access to
user_companies = []
is_super_admin = 'ST_APP_ROLE' in available_roles

if is_super_admin:
    user_companies = list(COMPANY_ROLES.values())
else:
    for role, company in COMPANY_ROLES.items():
        if role in available_roles:
            user_companies.append(company)

# Default company
if not user_companies:
    st.error("❌ You don't have access to any companies. Please contact your administrator.")
    st.stop()

# Initialize session state for company selection
if 'selected_company' not in st.session_state:
    st.session_state.selected_company = user_companies[0]

company = st.session_state.selected_company

# Get configuration for current company
config = USER_CONFIG.get(company, {})

# Initialize session state
initialize_session_state(company, config)

# ========================================
# SIDEBAR NAVIGATION
# ========================================
st.sidebar.title("🧭 Navigation")

# Company selector (if user has multiple companies)
# Company selector (if user has multiple companies)
if len(user_companies) > 1:
    st.sidebar.markdown("### 🏢 Company")
    selected_company = st.sidebar.selectbox(
        "Select Company",
        options=user_companies,
        index=user_companies.index(st.session_state.selected_company),
        label_visibility="collapsed",
        key="company_selector"
    )
    
    # Handle company change
    if selected_company != st.session_state.selected_company:
        # Store the current page before switching
        current_page = st.session_state.get('active_page', 'home')
        
        st.session_state.selected_company = selected_company
        clear_company_state()
        st.cache_data.clear()
        
        # 🔥 SMART PAGE PRESERVATION
        # Determine available pages for NEW company
        if selected_company == 'Zeus':
            new_available_pages = {
                "Home": "home",
                "Zeus": "zeus_adj",
                "Trigger Refresh": "Job_Orchestration"
            }
        else:
            new_available_pages = {
                "Home": "home",
                "COA Mapping": "coa",
                "Adjustments": "adjustments",
                "Trigger Refresh": "Job_Orchestration"
            }
        
        # Stay on current page if it exists in new company, otherwise go home
        if current_page in new_available_pages.values():
            st.session_state.active_page = current_page
        else:
            st.session_state.active_page = "home"
        
        st.rerun()

st.sidebar.markdown("---")

# Update company and config based on selection
company = st.session_state.selected_company
config = USER_CONFIG.get(company, {})

# ========================================
# PAGE SELECTION BASED ON COMPANY
# ========================================
st.sidebar.markdown("### 📄 Pages")

# Determine available pages based on company
if company == 'Zeus':
    available_pages = {
        "Home": "home",
        "Zeus": "zeus_adj",
        "Trigger Refresh": "Job_Orchestration"
    }
else:
    available_pages = {
        "Home": "home",
        "COA Mapping": "coa",
        "Adjustments": "adjustments",
        "Trigger Refresh": "Job_Orchestration"
    }

# Initialize or validate active_page
if 'active_page' not in st.session_state:
    st.session_state.active_page = "home"
elif st.session_state.active_page not in available_pages.values():
    # Fallback if somehow we're on an invalid page
    st.session_state.active_page = "home"

# Calculate current index safely
current_index = list(available_pages.values()).index(st.session_state.active_page)

# Page selection with company-specific key
selected_page_display = st.sidebar.radio(
    "Select Page",
    options=list(available_pages.keys()),
    index=current_index,
    label_visibility="collapsed",
    key=f"page_selector_{company}"
)

# Get the page key from display name
selected_page = available_pages[selected_page_display]

# Update active_page if changed
if selected_page != st.session_state.active_page:
    st.session_state.active_page = selected_page
    st.rerun()
st.sidebar.markdown("---")

# Show current user info in sidebar
st.sidebar.markdown("### 👤 User Info")
st.sidebar.caption(f"**User:** {current_user}")
st.sidebar.caption(f"**Company:** {company}")

# ========================================
# MAIN CONTENT AREA - ROUTE TO PAGES
# ========================================

# Route to appropriate page
if st.session_state.active_page == "home":
    # ========================================
    # HOME PAGE
    # ========================================
    st.title("🏠 Chart of Accounts Management")
    st.markdown("### Welcome to the COA Mapping and Adjustments System")
    
    st.info(f"""
    **Current User:** {current_user}  
    **Selected Company:** {company}  
    **Available Pages:** {len(available_pages) - 1}
    """)
    
    st.markdown("---")
    
    # Navigation instructions
    st.markdown("""
    ### 📋 Getting Started
    
    **Use the sidebar to:**
    - 🏢 Switch between companies (if you have access to multiple)
    - 📄 Navigate between pages
    
    **Available Features:**
    """)
    
    if company == 'Zeus':
        st.markdown("""
        - **📊 Zeus Adjustments**: Upload and manage Zeus transaction adjustments
          - Upload CSV files with metric hierarchies
          - View metric reference data
          - Bulk UPSERT operations
        """)
    else:
        st.markdown("""
        - **📋 COA Mapping**: Manage Chart of Accounts mappings
          - Edit mappings via UI with cascading dropdowns
          - Bulk upload via CSV
          - View metric and cashflow hierarchies
        
        - **📊 Adjustments**: Create and manage financial adjustments
          - Add/update individual records
          - Bulk edit existing records
          - CSV upload for bulk adjustments
        """)
    
    st.markdown("---")
    
    # Quick stats
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Company", company)
    
    with col2:
        st.metric("Available Pages", len(available_pages) - 1)
    
    with col3:
        if is_super_admin:
            st.metric("Access Level", "Super Admin")
        else:
            st.metric("Access Level", "Standard")
    
    # Configuration details
    with st.expander("🔧 Current Configuration", expanded=False):
        st.write("**Table Name:**", config.get("table_name", "N/A"))
        st.write("**Primary Keys:**", config.get("primary_keys", []))
        st.write("**Display Columns:**", config.get("display_columns", []))
        if company != 'Zeus':
            st.write("**Adjustments Table:**", config.get("adjustments_table", "N/A"))
    
    # Help section
    st.markdown("---")
    st.markdown("""
    ### 📞 Need Help?
    
    - **Documentation**: Check the user guide for detailed instructions
    - **Support**: Contact your system administrator
    - **Issues**: Report bugs or feature requests to the development team
    """)

elif st.session_state.active_page == "coa":
    # ========================================
    # COA MAPPING PAGE
    # ========================================
    if company == 'Zeus':
        st.error("❌ COA Mapping is not available for Zeus. Please use Zeus Adjustments.")
    else:
        # Import and run COA page
        exec(open("modules/COA.py").read())

elif st.session_state.active_page == "adjustments":
    # ========================================
    # ADJUSTMENTS PAGE
    # ========================================
    if company == 'Zeus':
        st.error("❌ Standard Adjustments are not available for Zeus. Please use Zeus Adjustments.")
    else:
        # Import and run Adjustments page
        exec(open("modules/Adjustments.py").read())

elif st.session_state.active_page == "Job_Orchestration":
    exec(open("modules/Job_orchestration.py").read())

elif st.session_state.active_page == "zeus_adj":
    # ========================================
    # ZEUS ADJUSTMENTS PAGE
    # ========================================
    if company != 'Zeus':
        st.error("❌ Zeus Adjustments are only available for Zeus company.")
    else:
        # Import and run Zeus Adjustments page
        exec(open("modules/Zeus.py").read())

else:
    st.error(f"❌ Unknown page: {st.session_state.active_page}")