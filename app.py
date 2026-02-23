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
        FROM AI_POC_DB.HEALTH_POLICY_POC.APP_USER_ACCESS
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
        FROM AI_POC_DB.HEALTH_POLICY_POC.DOCUMENT_METADATA
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
        FROM AI_POC_DB.HEALTH_POLICY_POC.DOCUMENT_METADATA
        WHERE LOB = '{lob}'
        AND STATE = '{state}'
        ORDER BY VERSION
    """).to_pandas()

    versions = version_df["VERSION"].tolist()
    version = st.sidebar.selectbox("Version", versions)

    search_btn = st.sidebar.button("🔍 Search")

    if search_btn:
        search_sql = f"""
            CALL AI_POC_DB.HEALTH_POLICY_POC.SEARCH_POLICY_CLAUSE(
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

    compare_lob = st.sidebar.selectbox("LOB", filters["LOB"], key="compare_lob")
    compare_state = st.sidebar.selectbox("State", filters["STATE"], key="compare_state")

    file_df = session.sql(f"""
        SELECT DISTINCT FILE_NAME
        FROM AI_POC_DB.HEALTH_POLICY_POC.DOCUMENT_METADATA
        WHERE LOB = '{compare_lob}'
        AND STATE = '{compare_state}'
        ORDER BY FILE_NAME
    """).to_pandas()

    filenames = file_df["FILE_NAME"].tolist()
    selected_file = st.sidebar.selectbox("Policy File Name", filenames)

    version_df = session.sql(f"""
        SELECT DISTINCT VERSION
        FROM AI_POC_DB.HEALTH_POLICY_POC.DOCUMENT_METADATA
        WHERE FILE_NAME = '{selected_file}'
        ORDER BY VERSION
    """).to_pandas()

    versions = version_df["VERSION"].tolist()

    if versions:
        latest_version = max(versions, key=lambda v: int(v.replace("v", "")))
    else:
        latest_version = None

    old_version = st.sidebar.selectbox("Old Version", versions)
    st.sidebar.write(f"Latest Version: {latest_version}")

    def get_doc_id(file_name, version):
        df = session.sql(f"""
            SELECT DOC_ID
            FROM AI_POC_DB.HEALTH_POLICY_POC.DOCUMENT_METADATA
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

        # -------------------------------------------------
        # Call Compare Procedure
        # -------------------------------------------------
        session.sql(f"""
            CALL AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.COMPARE_POLICY_VERSIONS(
                {old_doc_id},
                {new_doc_id}
            )
        """).collect()

        diff_df = session.sql(f"""
            SELECT *
            FROM AI_POC_DB.HEALTH_POLICY_POC_CHANGE_SUMMARY.POLICY_VERSION_DIFFS
            WHERE OLD_DOC_ID = {old_doc_id}
              AND NEW_DOC_ID = {new_doc_id}
            ORDER BY DIFF_ID
        """).to_pandas()

        if diff_df.empty:
            st.warning("No differences found.")
            st.stop()

        st.markdown("## 📝 Change Summary")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"### Old Version: {old_version}")
        with col2:
            st.markdown(f"### New Version: {latest_version}")

        st.markdown("---")

        for _, row in diff_df.iterrows():

            change_type = row["CHANGE_TYPE"]
            old_clause = row["OLD_CLAUSE"] if row["OLD_CLAUSE"] else ""
            new_clause = row["NEW_CLAUSE"] if row["NEW_CLAUSE"] else ""

            if change_type == "removed":
                old_color = "#f8d7da"
                new_color = "transparent"
            elif change_type == "added":
                old_color = "transparent"
                new_color = "#d4edda"
            elif change_type == "modified":
                old_color = "#fff3cd"
                new_color = "#fff3cd"
            else:
                old_color = "transparent"
                new_color = "transparent"

            col_old, col_new = st.columns(2)

            with col_old:
                if old_clause:
                    st.markdown(f"""
                    <div style="background-color:{old_color};
                                padding:15px;
                                border-radius:8px;
                                margin-bottom:10px;">
                        {old_clause}
                    </div>
                    """, unsafe_allow_html=True)

            with col_new:
                if new_clause:
                    st.markdown(f"""
                    <div style="background-color:{new_color};
                                padding:15px;
                                border-radius:8px;
                                margin-bottom:10px;">
                        {new_clause}
                    </div>
                    """, unsafe_allow_html=True)

# -------------------------------------------------
# Footer
# -------------------------------------------------
st.divider()
st.caption("Powered by Snowflake Cortex • Streamlit in Snowflake")
