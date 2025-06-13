from config import get_connection


class SnowflakeManager:
    def __init__(self):
        self.connection = get_connection()

    def execute(self, sql):
        with self.connection.cursor() as cur:
            cur.execute(sql)
            try:
                return cur.fetchall()
            except:
                return None

    def resume_warehouse(self, warehouse_name):
        self.execute(f"ALTER WAREHOUSE {warehouse_name} RESUME")

    def suspend_warehouse(self, warehouse_name):
        self.execute(f"ALTER WAREHOUSE {warehouse_name} SUSPEND")

    def resize_warehouse(self, warehouse_name, size):
        self.execute(f"ALTER WAREHOUSE {warehouse_name} SET WAREHOUSE_SIZE = '{size}'")

    def set_warehouse_timeout(self, warehouse_name, auto_suspend_sec, auto_resume=True):
        resume_clause = "AUTO_RESUME = TRUE" if auto_resume else "AUTO_RESUME = FALSE"
        self.execute(f"""
            ALTER WAREHOUSE {warehouse_name}
            SET AUTO_SUSPEND = {auto_suspend_sec},
            {resume_clause}
        """)

    def list_running_queries(self, min_duration_minutes=5):
        result = self.execute(f"""
            SELECT query_id, user_name, warehouse_name, start_time, execution_status, query_text
            FROM table(information_schema.query_history())
            WHERE execution_status = 'RUNNING'
              AND datediff('minute', start_time, current_timestamp()) >= {min_duration_minutes}
            ORDER BY start_time
        """)
        return result

    def abort_query(self, query_id):
        self.execute(f"SELECT system$cancel_query('{query_id}')")

    def close_connection(self):
        self.connection.close()
