import pyodbc
import credentials

def load_data(conn, query, data):
    cursor = conn.cursor()
    cursor.executemany(query, data)
    cursor.commit()
    cursor.close()

def connect_to_db():
    conn = pyodbc.connect('Driver={SQL Server};'
                  'Server='+credentials.db_endpoint+';'
                  'Database='+credentials.db_database+';'
                  'UID='+credentials.db_user+';'
                  'PWD='+credentials.db_password+';'
                  'Trusted_Connection=no;')
    return conn

def get_table_size(conn, table_name):
    cursor = conn.cursor()
    query = f"select COUNT(*) from {table_name}"
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    return results[0][0]