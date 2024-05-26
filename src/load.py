import pandas as pd
import teradatasql
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_data():
    try:
        # Read transformed data
        df = pd.read_csv('/path/to/silver/transformed_data.csv')
        logging.info("Transformed data read into DataFrame.")

        # Read database credentials from environment variables
        db_host = os.getenv('TD_HOST')
        db_user = os.getenv('TD_USER')
        db_password = os.getenv('TD_PASSWORD')
        db_database = os.getenv('TD_DATABASE')

        # Create connection
        conn = teradatasql.connect(host=db_host, user=db_user, password=db_password, database=db_database)
        cursor = conn.cursor()
        logging.info("Connected to Teradata database.")

        # Load data into Teradata table
        for index, row in df.iterrows():
            cursor.execute("""
                INSERT INTO target_table (
                    transaction_id, customer_id, product_id, amount, total_quantity, avg_amount,
                    customer_name, customer_email, transaction_date, amount_category, is_high_value, avg_quantity
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                row['transaction_id'], row['customer_id'], row['product_id'], row['amount'], row['total_quantity'],
                row['avg_amount'], row['customer_name'], row['customer_email'], row['transaction_date'],
                row['amount_category'], row['is_high_value'], row['avg_quantity']
            ))

        conn.commit()
        logging.info("Data loaded into target_table.")
        conn.close()

    except teradatasql.Error as e:
        logging.error(f"Error loading data into Teradata: {e}")
        raise

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise

if __name__ == "__main__":
    load_data()
