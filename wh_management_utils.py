from config import get_connection
import pandas as pd

def run_show_command_to_df(cur, command):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(command)
        results = cur.fetchall()
        columns = [col[0] for col in cur.description]
        return pd.DataFrame(results, columns=columns)
    finally:
        cur.close()

def manage_warehouse(action, warehouse_name, size=None, timeout=None):
    conn = get_connection()
    cur = conn.cursor()
    try:
        if action == "resume":
            cur.execute(f"ALTER WAREHOUSE {warehouse_name} RESUME")
        elif action == "suspend":
            cur.execute(f"ALTER WAREHOUSE {warehouse_name} SUSPEND")
        elif action == "resize" and size:
            cur.execute(f"ALTER WAREHOUSE {warehouse_name} SET WAREHOUSE_SIZE = '{size}'")
        elif action == "set_timeout" and timeout:
            cur.execute(f"ALTER WAREHOUSE {warehouse_name} SET AUTO_SUSPEND = {timeout}")
        else:
            raise ValueError("Invalid action or missing parameters.")
        conn.commit()
    finally:
        cur.close()
