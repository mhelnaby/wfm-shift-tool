import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import os

# Import database manager
from database.db_manager import DatabaseManager

# -------------------- Page config (must be first Streamlit command) --------------------
st.set_page_config(page_title="WFM Shift Tool", layout="wide")

# -------------------- Initialize database --------------------
db = DatabaseManager(year=2026, db_path="./data")
db.init_database()
db.ensure_monthly_tables("2026_02")

# -------------------- Session state initialization --------------------
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'user' not in st.session_state:
    st.session_state.user = None
if 'role' not in st.session_state:
    st.session_state.role = None

# -------------------- Login screen (if not authenticated) --------------------
if not st.session_state.authenticated:
    # Use a centered layout for login
    st.markdown("<style>div.block-container{max-width: 500px;}</style>", unsafe_allow_html=True)
    st.title("🔐 WFM Command Center Tool - Login")
    
    with st.form("login_form"):
        citrix_uid = st.text_input("Citrix UID")
        submit = st.form_submit_button("Login", type="primary")
        
        if submit and citrix_uid:
            with db.connect() as conn:
                cur = conn.execute("SELECT * FROM user_access WHERE citrix_uid = ?", (citrix_uid,))
                user = cur.fetchone()
            if user:
                st.session_state.authenticated = True
                st.session_state.user = dict(user)
                st.session_state.role = user['role']
                st.rerun()
            else:
                st.error("User not found")
    st.stop()  # Stop execution here if not authenticated

# -------------------- Main App (authenticated) --------------------
st.title("📊 WFM Command Center Tool")
st.caption(f"Welcome {st.session_state.user['full_name']} - ({st.session_state.role})")

# -------------------- Sidebar navigation --------------------
with st.sidebar:
    st.image("https://img.icons8.com/color/96/000000/calendar--v1.png", width=80)
    st.title("Menu")
    
    menu_options = ["Dashboard"]
    if st.session_state.role in ['ADMIN', 'LEAVES']:
        menu_options.append("Upload Files")
    if st.session_state.role in ['OPS', 'ADMIN']:
        menu_options.append("Swap Manager")
    if st.session_state.role in ['RTM', 'ADMIN']:
        menu_options.append("Approvals")
    if st.session_state.role == 'ADMIN':
        menu_options.extend(["Admin Panel", "Audit Trail"])
    menu_options.extend(["Export Data", "Reports", "Logout"])
    
    selected = st.radio("", menu_options, label_visibility="collapsed")
    
    if selected == "Logout":
        st.session_state.authenticated = False
        st.session_state.user = None
        st.session_state.role = None
        st.rerun()

# -------------------- Page content based on selection --------------------
if selected == "Dashboard":
    st.subheader("📈 Dashboard")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Agents", "0", "0")
    with col2:
        st.metric("Present Today", "0", "0")
    with col3:
        st.metric("Absent Today", "0", "0%")
    with col4:
        st.metric("Adherence", "0%", "0%")
    
    st.info("Here you will see today's statistics and notifications later.")

elif selected == "Upload Files":
    st.subheader("📤 Upload Files")
    tab1, tab2, tab3, tab4 = st.tabs(["Headcount", "Roster", "CMS", "Aspect/EIM"])
    
    # Import handlers once (they are used in multiple tabs)
    from modules.upload_handlers import UploadHandler
    from modules.normalization import ShiftNormalizer
    from modules.audit import AuditLogger
    
    normalizer = ShiftNormalizer(db)
    audit = AuditLogger(db)
    handler = UploadHandler(db, normalizer, audit)
    
    with tab1:
        st.write("##### Upload Headcount File")
        uploaded_file = st.file_uploader("Choose HC file", type=['csv', 'xlsx'], key="hc")
        if uploaded_file is not None:
            if st.button("Process HC"):
                with st.spinner("Processing..."):
                    result = handler.process_headcount(uploaded_file)
                    if result['success']:
                        st.success(f"Processed {result['agents_updated']} agents, added {result['new_agents']} new.")
                    else:
                        st.error(f"Processing failed: {result['error']}")

    with tab2:
        st.write("##### Upload Roster File")
        roster_file = st.file_uploader("Choose Roster file", type=['csv', 'xlsx'], key="roster")
        if roster_file is not None:
            try:
                # Preview file
                if roster_file.name.endswith('.csv'):
                    df = pd.read_csv(roster_file)
                else:
                    df = pd.read_excel(roster_file)
                st.write("Preview:")
                st.dataframe(df.head())
                
                # Auto‑detect columns based on your fixed structure
                fixed_cols = ['Name', 'Citrix UID']
                date_cols = [col for col in df.columns if col not in fixed_cols]
                
                st.info(f"Detected {len(date_cols)} date columns automatically.")
                
                if st.button("Process Roster", key="process_roster"):
                    if not date_cols:
                        st.error("No date columns found!")
                    else:
                        mapping = {
                            'name_col': 'Name',
                            'citrix_col': 'Citrix UID',
                            'acd_col': None,
                            'login_col': None,
                            'date_cols': date_cols
                        }
                        year_month = f"{datetime.now().year}_{datetime.now().month:02d}"
                        
                        # Reset file pointer
                        roster_file.seek(0)
                        
                        with st.spinner("Processing..."):
                            result = handler.process_roster(roster_file, mapping, year_month)
                        if result['success']:
                            st.success(f"Processed {result['rows_processed']} shifts.")
                            if result['unknown_agents']:
                                st.warning(f"Unknown agents: {', '.join(result['unknown_agents'][:5])}")
                        else:
                            st.error(f"Processing failed: {result['error']}")
            except Exception as e:
                st.error(f"Error reading file: {e}")

    with tab3:
        st.write("##### Upload CMS Report")
        cms_file = st.file_uploader("Choose CMS file", type=['txt', 'csv'], key="cms")
        if cms_file and st.button("Process CMS", key="process_cms"):
            with st.spinner("Processing..."):
                year_month = datetime.now().strftime('%Y_%m')
                result = handler.process_cms_productivity(cms_file, year_month)
                if result['success']:
                    st.success(f"✅ Processed {result['rows_processed']} rows.")
                    if result['unknown_agents']:
                        st.warning(f"Unknown agents: {', '.join(result['unknown_agents'][:5])}")
                else:
                    st.error(f"❌ Failed: {result['error']}")

    with tab4:
        st.write("##### Upload Aspect/EIM Report")
        aspect_file = st.file_uploader("Choose Aspect/EIM file", type=['txt', 'csv'], key="aspect")
        if aspect_file and st.button("Process Aspect/EIM", key="process_aspect"):
            with st.spinner("Processing..."):
                year_month = datetime.now().strftime('%Y_%m')
                # Determine if it's Aspect or EIM based on filename
                if 'eim' in aspect_file.name.lower():
                    result = handler.process_eim(aspect_file, year_month)
                else:
                    result = handler.process_aspect(aspect_file, year_month)
                if result['success']:
                    st.success(f"✅ Processed {result['rows_processed']} events.")
                    if result['unknown_agents']:
                        st.warning(f"Unknown agents: {', '.join(result['unknown_agents'][:5])}")
                else:
                    st.error(f"❌ Failed: {result['error']}")

elif selected == "Swap Manager":
    st.subheader("🔄 Swap Requests")
    
    with st.form("swap_request"):
        st.write("##### New Swap Request")
        col1, col2 = st.columns(2)
        with col1:
            agent_acd = st.text_input("Agent ACD ID")
            swap_date = st.date_input("Swap Date")
        with col2:
            new_shift = st.text_input("New Shift (or leave type)")
            leave_type = st.selectbox("Leave Type", ["None", "Sick", "Annual", "Half Day Annual", "Casual", "Ops Update"])
        
        submitted = st.form_submit_button("Submit Request")
        if submitted:
            st.success("Request submitted successfully (demo)")

elif selected == "Approvals":
    st.subheader("✅ Pending Approvals")
    st.info("No pending requests at the moment.")

elif selected == "Admin Panel":
    st.subheader("⚙️ System Settings")
    
    tab1, tab2, tab3 = st.tabs(["Users", "Shift Dictionary", "LOB Groups"])
    
    with tab1:
        st.write("##### User Management")
        with db.connect() as conn:
            users_df = pd.read_sql_query("SELECT citrix_uid, role, full_name, email, is_active FROM user_access", conn)
        st.dataframe(users_df, use_container_width=True)
        
        with st.expander("Add New User"):
            new_citrix = st.text_input("Citrix UID")
            new_name = st.text_input("Full Name")
            new_role = st.selectbox("Role", ["OPS", "RTM", "ADMIN", "LEAVES"])
            new_email = st.text_input("Email")
            if st.button("Add"):
                with db.connect() as conn:
                    conn.execute("""
                        INSERT OR REPLACE INTO user_access (citrix_uid, role, full_name, email, created_at)
                        VALUES (?, ?, ?, ?, ?)
                    """, (new_citrix, new_role, new_name, new_email, datetime.now()))
                    conn.commit()
                st.success("User added successfully")
                st.rerun()
    
    with tab2:
        st.write("##### Shift Dictionary")
        st.info("Here you will manage shift patterns.")
    
    with tab3:
        st.write("##### LOB Groups")
        st.info("Here you will manage LOB groups.")

elif selected == "Audit Trail":
    st.subheader("📋 Audit Log")
    st.info("All system events will be shown here.")

elif selected == "Export Data":
    st.subheader("📥 Export Data")
    col1, col2 = st.columns(2)
    with col1:
        st.write("##### Export Options")
        export_type = st.radio("Export Type", ["Roster", "Attendance", "Absenteeism"])
        start_date = st.date_input("Start Date")
        end_date = st.date_input("End Date")
    with col2:
        st.write("##### Preview")
        st.info("Preview will be shown here.")
    
    if st.button("Export"):
        st.success("Export completed successfully (demo)")

elif selected == "Reports":
    st.subheader("📋 Reports")
    report_type = st.selectbox("Report Type", ["Absenteeism Report", "Adherence Report", "Swap Report"])
    if st.button("Generate Report"):
        st.info("Report will be generated here (demo)")