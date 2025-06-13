import streamlit as st
import re
import pandas as pd
from config import get_connection
from wh_management_utils import run_show_command_to_df, manage_warehouse

# Connection & cursor
conn = get_connection()

def run_query(query):
    df = pd.read_sql(query, conn)
    return df

def run_query_single(query):
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchone()
    cur.close()
    return result

def count_joins_in_text(query_text):
    if query_text:
        joins = re.findall(r'\bJOIN\b', query_text, flags=re.IGNORECASE)
        return len(joins)
    else:
        return 0

st.set_page_config(page_title="Warehouse Monitoring Dashboard", layout="wide")

st.markdown("""
    <style>
    .block-container { max-width: 1150px; margin: auto; padding-top: 1rem; padding-bottom: 2rem; }
    </style>
    """, unsafe_allow_html=True)

st.sidebar.title(" Choose an option 🎲 ")
section = st.sidebar.radio("", [
    "Live Dashboard 📈",
    "Credit Usage Overview 💰",
    "Long-Running Queries 🏃🏻‍♀️‍➡️",
    "Bytes Scanned & Cache Hit % 🎯",
    "Local Spill Analysis 🫗",
    "Remote Spill Analysis 🍾",
    "Warehouse Load Summary 🏋🏻‍♂️",
    "Cluster Config (Min/Max) ✨",
    "Queued Time Analysis ⏳",
    "Warehouse Management 🔧"
])

# Existing sections unchanged...

# Sections
if section == "Credit Usage Overview 💰":
    st.subheader("Credit Usage (Last 24H)")
    query = """
    SELECT
    WAREHOUSE_NAME,
    SUM(CREDITS_USED) AS TOTAL_CREDITS
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    GROUP BY WAREHOUSE_NAME
    ORDER BY TOTAL_CREDITS DESC;
    """
    st.dataframe(run_query(query))

elif section == "Long-Running Queries 🏃🏻‍♀️‍➡️":
    st.subheader("Long-Running Queries (>5 min, Last 24H)")
    query = """
    SELECT QUERY_ID, USER_NAME, WAREHOUSE_NAME, TOTAL_ELAPSED_TIME/60000 AS MINUTES
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP)
      AND TOTAL_ELAPSED_TIME >= 300000
    ORDER BY MINUTES DESC;
    """
    st.dataframe(run_query(query))

    selected_query_id = st.text_input("Enter a Query ID to inspect:")

    if selected_query_id:
        detail_query = f"""
        SELECT QUERY_TEXT
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE QUERY_ID = '{selected_query_id}'
        """
        result = run_query_single(detail_query)

        if result:
            query_text = result[0]
            join_count = count_joins_in_text(query_text)
            st.write(f"**Estimated number of JOINs in query:** {join_count}")
            st.text_area("Query Text", query_text, height=300)
        else:
            st.write("Query not found or still in history ingestion window.")

elif section == "Bytes Scanned & Cache Hit % 🎯":
    st.subheader("Bytes Scanned & Cache Usage (Last 24H)")
    query = """
    SELECT QUERY_ID,
       BYTES_SCANNED / 1024 / 1024 AS BYTES_SCANNED, PERCENTAGE_SCANNED_FROM_CACHE  / 1024 / 1024 AS CACHE_HIT,
       (PERCENTAGE_SCANNED_FROM_CACHE / NULLIF(BYTES_SCANNED, 0)) * 100 AS CACHE_HIT_PERCENT
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP)
    ORDER BY BYTES_SCANNED DESC
    LIMIT 10;
    """
    st.dataframe(run_query(query))

elif section == "Local Spill Analysis 🫗":
    st.subheader("Top 10 Queries with Local Spill (Last 24H)")
    query = """
    SELECT QUERY_ID, USER_NAME, WAREHOUSE_NAME,
           BYTES_SPILLED_TO_LOCAL_STORAGE / 1024 / 1024 AS LOCAL_SPILL
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP)
      AND BYTES_SPILLED_TO_LOCAL_STORAGE > 0
    ORDER BY LOCAL_SPILL DESC
    LIMIT 10;
    """
    st.dataframe(run_query(query))

elif section == "Remote Spill Analysis 🍾":
    st.subheader("Top 10 Queries with Remote Spill (Last 24H)")
    query = """
    SELECT QUERY_ID, USER_NAME, WAREHOUSE_NAME,
           BYTES_SPILLED_TO_REMOTE_STORAGE / 1024 / 1024 AS REMOTE_SPILL
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP)
      AND BYTES_SPILLED_TO_REMOTE_STORAGE > 0
    ORDER BY REMOTE_SPILL DESC
    LIMIT 10;
    """
    st.dataframe(run_query(query))

elif section == "Warehouse Load Summary 🏋🏻‍♂️":
    st.subheader("Warehouse Load Summary (Last 24H)")
    query = """
    SELECT WAREHOUSE_NAME,
           AVG(AVG_RUNNING) AS AVG_RUNNING_QUERIES,
           AVG(AVG_QUEUED_LOAD) AS AVG_QUEUE_LOAD,
           AVG(AVG_QUEUED_PROVISIONING) AS AVG_PROVISIONING_TIME_SECONDS,
           AVG(AVG_RUNNING) AS AVG_RUNNING,
           AVG(AVG_BLOCKED) AS AVG_BLOCKED_QUERIES
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
    WHERE START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP)
    GROUP BY WAREHOUSE_NAME
    ORDER BY AVG_QUEUE_LOAD  DESC;
    """
    st.dataframe(run_query(query))

elif section == "Queued Time Analysis ⏳":
    st.subheader("Warehouse Queued Time Metrics (Last 24H)")
    query = """
    SELECT WAREHOUSE_NAME,
           AVG(AVG_RUNNING) AS AVG_RUNNING,
           AVG(AVG_QUEUED_LOAD) AS AVG_QUEUE_LOAD,
           AVG(AVG_QUEUED_PROVISIONING) AS AVG_PROVISIONING_TIME_SECONDS
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
    WHERE START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP)
    GROUP BY WAREHOUSE_NAME
    ORDER BY AVG_QUEUE_LOAD DESC;
    """
    st.dataframe(run_query(query))

elif section == "Cluster Config (Min/Max) ✨":
    st.subheader("Warehouse Cluster Min/Max Settings")

    cur = conn.cursor()
    try:
        df = run_show_command_to_df(cur, "SHOW WAREHOUSES")

        df_filtered = df[["name", "min_cluster_count", "max_cluster_count", "scaling_policy", "state"]]
        df_filtered = df_filtered.rename(columns={
            "name": "WAREHOUSE_NAME",
            "min_cluster_count": "MIN_CLUSTER_COUNT",
            "max_cluster_count": "MAX_CLUSTER_COUNT",
            "scaling_policy": "SCALING_POLICY",
            "state": "ENABLED"
        }).sort_values("MAX_CLUSTER_COUNT", ascending=False)

        st.dataframe(df_filtered)

    finally:
        cur.close()

elif section == "Live Dashboard 📈":
    st.subheader("Live Warehouse & Query Monitoring (Last 10 min)")

    # Active Warehouse State
    st.write("### Warehouse State")

    cur = conn.cursor()
    try:
        df_wh_state = run_show_command_to_df(cur, "SHOW WAREHOUSES")
    finally:
        cur.close()
    st.dataframe(df_wh_state[['name', 'state', 'size', 'running', 'queued', 'scaling_policy']])

    # Active Queries in last 10 min
    st.write("### Active Queries (Last 10 min)")
    query = """
    SELECT QUERY_ID, USER_NAME, WAREHOUSE_NAME, EXECUTION_STATUS,
           TOTAL_ELAPSED_TIME/1000 AS SECONDS_ELAPSED
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= DATEADD('minute', -10, CURRENT_TIMESTAMP)
      AND EXECUTION_STATUS IN ('RUNNING', 'QUEUED')
    ORDER BY START_TIME DESC;
    """
    st.dataframe(run_query(query))

    # Warehouse Load (Last 10 min)
    st.write("### Warehouse Load Metrics (Last 10 min)")
    query = """
    SELECT WAREHOUSE_NAME,
           AVG(AVG_RUNNING) AS AVG_RUNNING,
           AVG(AVG_QUEUED_LOAD) AS AVG_QUEUED_LOAD,
           AVG(AVG_QUEUED_PROVISIONING) AS AVG_PROVISIONING_TIME
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
    WHERE START_TIME >= DATEADD('minute', -10, CURRENT_TIMESTAMP)
    GROUP BY WAREHOUSE_NAME
    ORDER BY AVG_QUEUED_LOAD DESC;
    """
    st.dataframe(run_query(query))

    if st.button("🔄 Refresh the Dashboard", type="tertiary"):
        st.rerun()

elif section == "Warehouse Management 🔧":
    st.subheader("Warehouse Management Console")

    # Open cursor, run SHOW WAREHOUSES, close cursor
    cur = conn.cursor()
    df_wh = run_show_command_to_df(cur, "SHOW WAREHOUSES")
    cur.close()

    # Display warehouse list and management actions
    wh_list = df_wh["name"].tolist()
    selected_wh = st.selectbox("Select a Warehouse", wh_list)

    if selected_wh:
        wh_details = df_wh[df_wh["name"] == selected_wh].iloc[0]

        st.write(f"**Size:** {wh_details['size']}")
        st.write(f"**State:** {wh_details['state']}")
        st.write(f"**Auto Suspend:** {wh_details['auto_suspend']}")
        st.write(f"**Auto Resume:** {wh_details['auto_resume']}")
        st.write(f"**Min Clusters:** {wh_details['min_cluster_count']}")
        st.write(f"**Max Clusters:** {wh_details['max_cluster_count']}")

        st.markdown("---")

        # Management actions
        if st.button("Resume Warehouse 🚀"):
            cur = conn.cursor()
            cur.execute(f"ALTER WAREHOUSE {selected_wh} RESUME")
            cur.close()
            st.success(f"{selected_wh} resumed.")

        if st.button("Suspend Warehouse 💤"):
            cur = conn.cursor()
            cur.execute(f"ALTER WAREHOUSE {selected_wh} SUSPEND")
            cur.close()
            st.success(f"{selected_wh} suspended.")

        new_size = st.selectbox("Resize to:", ["XSMALL", "SMALL", "MEDIUM", "LARGE", "XLARGE", "XXLARGE"])
        if st.button("Apply Resize 📏"):
            cur = conn.cursor()
            cur.execute(f"ALTER WAREHOUSE {selected_wh} SET WAREHOUSE_SIZE = {new_size}")
            cur.close()
            st.success(f"{selected_wh} resized to {new_size}.")

        new_timeout = st.number_input("Auto Suspend Timeout (seconds)", min_value=60, step=60, value=int(wh_details['auto_suspend']))
        if st.button("Update Auto Suspend ⏳"):
            cur = conn.cursor()
            cur.execute(f"ALTER WAREHOUSE {selected_wh} SET AUTO_SUSPEND = {int(new_timeout)}")
            cur.close()
            st.success(f"{selected_wh} auto_suspend timeout set to {int(new_timeout)} seconds.")
            

        new_stmt_timeout = st.number_input("Statement Timeout (seconds)", min_value=0, step=10, value=3600)
        if st.button("Update Query Timeout 📄⏱️"):
            cur = conn.cursor()
            cur.execute(f"ALTER WAREHOUSE {selected_wh} SET STATEMENT_TIMEOUT_IN_SECONDS = {int(new_stmt_timeout)}")
            cur.close()
            st.success(f"{selected_wh} Query Timeout updated to {int(new_stmt_timeout)} seconds.")


        st.markdown("---")
        if st.button("🔄 Refresh Warehouse List"):
            st.rerun()


