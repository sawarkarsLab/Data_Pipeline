import pandas as pd
import cx_Oracle

def extract_data():
    dsn = cx_Oracle.makedsn("hostname", "port", sid="sid")
    conn = cx_Oracle.connect(user="username", password="password", dsn=dsn)

    query = "SELECT * FROM source_table"
    df = pd.read_sql(query, conn)
    conn.close()

    df.to_csv('/path/to/bronze/raw_data.csv', index=False)
    return '/path/to/bronze/raw_data.csv'
