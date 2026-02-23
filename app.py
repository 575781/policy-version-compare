import streamlit as st
from snowflake.snowpark.context import get_active_session
from datetime import datetime
import json

# -------------------------------------------------
# Page Configuration
# -------------------------------------------------
st.set_page_config(
    page_title="Policy & Control Search",
    layout="wide"
)

# -------------------------------------------------
# SAFE Session State Initialization
# -------------------------------------------------
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

if "username" not in st.session_state:
    st.session_state["username"] = None

if "app_role" not in st.session_state:
    st.session_state["app_role"] = None

# -------------------------------------------------
# Get Snowflake Session
# -------------------------------------------------
session = get_active_session()

# -------------------------------------------------
# Helper: Fetch App Role
# -------------------------------------------------
def get_app_role(user_name):
    df = session.sql("""
        SELECT APP_ROLE
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.APP_USER_ACCESS
        WHERE (
            UPPER(USER_NAME) = UPPER(:1)
            OR UPPER(USER_NAME) = SPLIT(UPPER(:1), '@')[0]
        )
        AND IS_ACTIVE = TRUE
    """, [user_name]).to_pandas()

    if df.empty:
        return None

    return df.iloc[0]["APP_ROLE"]

# -------------------------------------------------
# Helper: Load Filter Values
# -------------------------------------------------
def load_filter_values():
    df = session.sql("""
        SELECT DISTINCT
            LOB,
            STATE
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
        ORDER BY 1,2
    """).to_pandas()

    return {
        "LOB": sorted(df["LOB"].dropna().unique().tolist()),
        "STATE": sorted(df["STATE"].dropna().unique().tolist())
    }

# -------------------------------------------------
# LOGIN SCREEN
# -------------------------------------------------
if not st.session_state["authenticated"]:

    st.title("🔐 Policy Search Login")

    with st.form("login_form"):
        login_user = st.text_input("Username")
        login_btn = st.form_submit_button("Login")

    if login_btn:

        if not login_user.strip():
            st.warning("Please enter your username.")
            st.stop()

        role = get_app_role(login_user)

        if not role:
            st.error("❌ You are not authorized to access this application.")
            st.stop()

        st.session_state["authenticated"] = True
        st.session_state["username"] = login_user
        st.session_state["app_role"] = role

    st.stop()

# -------------------------------------------------
# USER CONTEXT
# -------------------------------------------------
current_user = st.session_state["username"]
app_role = st.session_state["app_role"]

current_role = session.sql(
    "SELECT CURRENT_ROLE()"
).collect()[0][0]

# -------------------------------------------------
# Sidebar – User Info
# -------------------------------------------------
st.sidebar.success("Authenticated")
st.sidebar.write("👤 User:", current_user)
st.sidebar.write("🛡️ App Role:", app_role.upper())

if st.sidebar.button("🚪 Logout"):
    st.session_state.clear()

# -------------------------------------------------
# Sidebar – Menu
# -------------------------------------------------
st.sidebar.header("📂 Menu")

app_mode = st.sidebar.radio(
    "Select Option",
    ["Search Policy", "Analyze Policy Changes"]
)

st.title("📄 Policy & Control Search")

filters = load_filter_values()

# =================================================
# SEARCH MODE
# =================================================
if app_mode == "Search Policy":

    st.sidebar.header("🔎 Search Filters")

    search_text = st.sidebar.text_input("Search Query")

    lob = st.sidebar.selectbox("LOB", filters["LOB"])
    state = st.sidebar.selectbox("State", filters["STATE"])

    version_df = session.sql(f"""
        SELECT DISTINCT VERSION
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
        WHERE LOB = '{lob}'
        AND STATE = '{state}'
        ORDER BY VERSION
    """).to_pandas()

    versions = version_df["VERSION"].tolist()

    version = st.sidebar.selectbox("Version", versions)

    search_btn = st.sidebar.button("🔍 Search")

    if search_btn:

        search_sql = f"""
            CALL AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.SEARCH_POLICY_CLAUSE(
                '{search_text}',
                '{state}',
                '{lob}',
                '{version}'
            )
        """

        results_df = session.sql(search_sql).to_pandas()

        if results_df.empty:
            st.warning("No matching clauses found.")
        else:
            st.dataframe(results_df)

# =================================================
# ANALYZE POLICY CHANGES
# =================================================
if app_mode == "Analyze Policy Changes":

    st.title("🔄 Analyze Policy Changes")

    st.sidebar.header("🧩 Comparison Filters")

    compare_lob = st.sidebar.selectbox(
        "LOB",
        filters["LOB"],
        key="compare_lob"
    )

    compare_state = st.sidebar.selectbox(
        "State",
        filters["STATE"],
        key="compare_state"
    )

    file_df = session.sql(f"""
        SELECT DISTINCT FILE_NAME
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
        WHERE LOB = '{compare_lob}'
        AND STATE = '{compare_state}'
        ORDER BY FILE_NAME
    """).to_pandas()

    filenames = file_df["FILE_NAME"].tolist()

    selected_file = st.sidebar.selectbox(
        "Policy File Name",
        filenames,
        key="selected_file"
    )

    version_df = session.sql(f"""
        SELECT DISTINCT VERSION
        FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
        WHERE FILE_NAME = '{selected_file}'
        ORDER BY VERSION
    """).to_pandas()

    versions = version_df["VERSION"].tolist()

    latest_version = sorted(versions)[-1] if versions else None

    old_version = st.sidebar.selectbox(
        "Old Version",
        versions,
        key="old_version"
    )

    st.sidebar.write(f"Latest Version: {latest_version}")

    def get_doc_id(file_name, version):
        df = session.sql(f"""
            SELECT DOC_ID
            FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.DOCUMENT_METADATA
            WHERE FILE_NAME = '{file_name}'
            AND VERSION = '{version}'
        """).to_pandas()

        if df.empty:
            return None
        return df.iloc[0]["DOC_ID"]

    old_doc_id = get_doc_id(selected_file, old_version)
    new_doc_id = get_doc_id(selected_file, latest_version)

    analyze_btn = st.sidebar.button("Analyze Policy Impact")

    if analyze_btn:

        st.subheader("Selected Comparison")

        st.write("Policy File:", selected_file)
        st.write("Old Version:", old_version)
        st.write("Latest Version:", latest_version)
        st.write("Old DOC_ID:", old_doc_id)
        st.write("New DOC_ID:", new_doc_id)

# -------------------------------------------------
# Footer
# -------------------------------------------------
st.divider()
st.caption("Powered by Snowflake Cortex • Streamlit in Snowflake")
