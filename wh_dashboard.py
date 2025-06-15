import streamlit as st
import pandas as pd
import snowflake_manager as sfm

# UI Layout

st.set_page_config(
    page_title="Warehouse Monitoring Dashboard",
    page_icon="❄️",
    layout="centered",  # This makes the app use the full width of the browser
    initial_sidebar_state="auto"
)

# Custom CSS to limit content width while still using 'wide' layout

st.markdown("""
    <style>
    .main .block-container {
        max-width: 1200px;
        min-width: 1000px;
        padding-top: 2rem;
        padding-bottom: 2rem;
        margin: 0 auto;
    }
    </style>
""", unsafe_allow_html=True)

st.title("Warehouse Monitoring Dashboard")


st.sidebar.title(" Choose an option 🎲 ")
section = st.sidebar.radio("", 
    [
        "Live Dashboard 📈",
        "Credit Usage Overview 💰",
        "Long-Running Queries 🏃🏻‍♀️‍➡️",
        "Bytes Scanned & Cache Hit % 🎯",
        "Local Spill Analysis 🫗",
        "Remote Spill Analysis 🍾",
        "Warehouse Load Summary 🏋🏻‍♂️",
        "Queued Time Analysis ⏳",
        "Warehouse Management 🔧",
    ]
)

if section == "Live Dashboard 📈":
    st.subheader("Live Warehouse Load")
    st.dataframe(sfm.get_live_warehouse_load())
    st.subheader("Live Queries (Running / Queued)")
#    st.dataframe(sfm.get_live_queries())
    df = sfm.get_live_queries()
    st.dataframe(df)
    if st.button("🔄 Refresh the Dashboard", type="tertiary"):
            st.rerun()

elif section == "Credit Usage Overview 💰":
    st.subheader("Warehouse Credit Usage")
    st.dataframe(sfm.get_credit_usage())

elif section == "Long-Running Queries 🏃🏻‍♀️‍➡️":
    st.subheader("Long-Running Queries (>5 min)")
    df = sfm.get_long_running_queries()
    st.dataframe(df)

    query_id = st.text_input("Enter a Query ID to view its text:")
    if query_id:
        result = sfm.get_query_text_by_id(query_id)
        if result:
            st.code(result[0], language='sql')
        else:
            st.warning("Query ID not found.")

elif section == "Bytes Scanned & Cache Hit % 🎯":
    st.subheader("Bytes Scanned & Cache Usage")
    st.dataframe(sfm.get_bytes_scanned_and_cache())

elif section == "Local Spill Analysis 🫗":
    st.subheader("Local Spill (MB)")
    st.dataframe(sfm.get_local_spill())

elif section == "Remote Spill Analysis 🍾":
    st.subheader("Remote Spill (MB)")
    st.dataframe(sfm.get_remote_spill())

elif section == "Warehouse Load Summary 🏋🏻‍♂️":
    st.subheader("Warehouse Load (Last 24 hrs)")
    st.dataframe(sfm.get_warehouse_load_summary())

elif section == "Queued Time Analysis ⏳":
    st.subheader("Query Queue Time & Load (Last 24 hrs)")
    st.dataframe(sfm.get_queued_time_analysis())

elif section == "Warehouse Management 🔧":
    st.subheader("Warehouse Management Console")

    df_wh = sfm.run_show_command_to_df("SHOW WAREHOUSES")
    wh_list = df_wh["name"].tolist()

    wh_selected = st.selectbox("Select a warehouse to manage", wh_list)

    if wh_selected:
        wh_df = df_wh[df_wh["name"] == wh_selected].reset_index(drop=True)
        st.dataframe(wh_df)
       

#        st.write("### Warehouse Management Controls")

        col1, col2, col3 = st.columns(3)

        with col1:
            if col1.button("Resume Warehouse"):
                sfm.resume_warehouse(wh_selected)
                st.success(f"{wh_selected} resumed.")
                
                
        with col3:
            if col3.button("Suspend Warehouse"):
                sfm.suspend_warehouse(wh_selected)
                st.success(f"{wh_selected} suspended.")

        col1, col2, col3 = st.columns(3)

        with col2:
            new_size = st.selectbox("Resize to", ['XSMALL', 'SMALL', 'MEDIUM', 'LARGE', 'XLARGE', 'XXLARGE'])
            if st.button("Resize Warehouse"):
                sfm.resize_warehouse(wh_selected, new_size)
                st.success(f"{wh_selected} resized to {new_size}.")

        col1, col2, col3 = st.columns(3)
        
        with col2:
            new_auto_suspend = st.number_input("New AUTO_SUSPEND (seconds)", min_value=0, value=300, step=30)
            if st.button("Update Auto Suspend"):
                sfm.set_auto_suspend(wh_selected, new_auto_suspend)
                st.success(f"{wh_selected} AUTO_SUSPEND set to {new_auto_suspend} sec.")
                
        col1, col2, col3 = st.columns(3)

        with col2:
            new_stmt_timeout = st.number_input("New STATEMENT_TIMEOUT_IN_SECONDS", min_value=0, value=600, step=60)
            if st.button("Update Query Timeout"):
                sfm.set_statement_timeout(wh_selected, new_stmt_timeout)
                st.success(f"{wh_selected} STATEMENT_TIMEOUT_IN_SECONDS set to {new_stmt_timeout} sec.")
                

                
st.sidebar.caption("Ads Warehouse Monitoring Dashboard")
