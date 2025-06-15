import pandas as pd
import re
from config import get_connection


# ================== Query Execution Functions ==================

def run_query(query):
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def run_show_command_to_df(command):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(command)
    results = cur.fetchall()
    columns = [col[0] for col in cur.description]
    cur.close()
    conn.close()
    return pd.DataFrame(results, columns=columns)

def run_query_single(query):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchone()
    cur.close()
    conn.close()
    return result

def count_joins_in_text(query_text):
    if query_text:
        joins = re.findall(r'\bJOIN\b', query_text, flags=re.IGNORECASE)
        return len(joins)
    else:
        return 0

# ================== Warehouse Metrics Queries ==================

def get_credit_usage():
    query = """
    SELECT WAREHOUSE_NAME, SUM(CREDITS_USED) AS TOTAL_CREDITS
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    GROUP BY WAREHOUSE_NAME
    ORDER BY TOTAL_CREDITS DESC;
    """
    return run_query(query)

def get_long_running_queries():
    query = """
    SELECT QUERY_ID, USER_NAME, WAREHOUSE_NAME, TOTAL_ELAPSED_TIME/60000 AS MINUTES
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP)
      AND TOTAL_ELAPSED_TIME >= 300000
    ORDER BY MINUTES DESC;
    """
    return run_query(query)

def get_query_text_by_id(query_id):
    query = f"""
    SELECT QUERY_TEXT
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE QUERY_ID = '{query_id}'
    """
    return run_query_single(query)

def get_bytes_scanned_and_cache():
    query = """
    SELECT QUERY_ID,
       BYTES_SCANNED / 1024 / 1024 AS BYTES_SCANNED,
       PERCENTAGE_SCANNED_FROM_CACHE  / 1024 / 1024 AS CACHE_HIT,
       (PERCENTAGE_SCANNED_FROM_CACHE / NULLIF(BYTES_SCANNED, 0)) * 100 AS CACHE_HIT_PERCENT
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP)
    ORDER BY BYTES_SCANNED DESC
    LIMIT 10;
    """
    return run_query(query)

def get_local_spill():
    query = """
    SELECT QUERY_ID, USER_NAME, WAREHOUSE_NAME,
           BYTES_SPILLED_TO_LOCAL_STORAGE / 1024 / 1024 AS LOCAL_SPILL
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP)
      AND BYTES_SPILLED_TO_LOCAL_STORAGE > 0
    ORDER BY LOCAL_SPILL DESC
    LIMIT 10;
    """
    return run_query(query)

def get_remote_spill():
    query = """
    SELECT QUERY_ID, USER_NAME, WAREHOUSE_NAME,
           BYTES_SPILLED_TO_REMOTE_STORAGE / 1024 / 1024 AS REMOTE_SPILL
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP)
      AND BYTES_SPILLED_TO_REMOTE_STORAGE > 0
    ORDER BY REMOTE_SPILL DESC
    LIMIT 10;
    """
    return run_query(query)

def get_warehouse_load_summary():
    query = """
    SELECT WAREHOUSE_NAME,
           AVG(AVG_RUNNING) AS AVG_RUNNING_QUERIES,
           AVG(AVG_QUEUED_LOAD) AS AVG_QUEUE_LOAD,
           AVG(AVG_QUEUED_PROVISIONING) AS AVG_PROVISIONING_TIME_SECONDS,
           AVG(AVG_BLOCKED) AS AVG_BLOCKED_QUERIES
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
    WHERE START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP)
    GROUP BY WAREHOUSE_NAME
    ORDER BY AVG_QUEUE_LOAD  DESC;
    """
    return run_query(query)

def get_queued_time_analysis():
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
    return run_query(query)

def get_live_queries():
    query = """
    SELECT QUERY_ID, USER_NAME, WAREHOUSE_NAME, EXECUTION_STATUS,
           TOTAL_ELAPSED_TIME/1000 AS SECONDS_ELAPSED
    FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY())
    WHERE START_TIME <= DATEADD('minute', -10, CURRENT_TIMESTAMP)
      AND EXECUTION_STATUS IN ('RUNNING', 'QUEUED')
    ORDER BY START_TIME DESC;
    """
    return run_query(query)

def get_live_warehouse_load():
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
    return run_query(query)


# ================== Warehouse Management Functions ==================


def resume_warehouse(warehouse_name):
    query = f"ALTER WAREHOUSE {warehouse_name} RESUME;"
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.close()

def suspend_warehouse(warehouse_name):
    query = f"ALTER WAREHOUSE {warehouse_name} SUSPEND;"
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.close()

def resize_warehouse(warehouse_name, new_size):
    query = f"ALTER WAREHOUSE {warehouse_name} SET WAREHOUSE_SIZE = '{new_size}'"
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.close()


def set_auto_suspend(warehouse_name, auto_suspend_seconds):
    query = f"ALTER WAREHOUSE {warehouse_name} SET AUTO_SUSPEND = {auto_suspend_seconds}"
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.close()


def set_statement_timeout(warehouse_name, statement_timeout_seconds):
    query = f"ALTER WAREHOUSE {warehouse_name} SET STATEMENT_TIMEOUT_IN_SECONDS = {statement_timeout_seconds}"
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.close()
