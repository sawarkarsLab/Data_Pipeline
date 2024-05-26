import pandas as pd
import teradatasql

def load_data():
    df = pd.read_csv('/path/to/silver/transformed_data.csv')

    conn = teradatasql.connect(host='hostname', user='username', password='password', database='database_name')
    cursor = conn.cursor()
    
    for index, row in df.iterrows():
        cursor.execute("INSERT INTO target_table (col1, col2, ...) VALUES (?, ?, ...)", (row['col1'], row['col2'], ...))

    conn.close()
