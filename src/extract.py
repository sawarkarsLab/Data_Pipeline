import pandas as pd
import cx_Oracle
import logging
import datetime
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_data():
    try:
        # Read database credentials from environment variables
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        db_service_name = os.getenv('DB_SERVICE_NAME')

        # Create a DSN (Data Source Name) for Oracle connection
        dsn = cx_Oracle.makedsn(db_host, db_port, service_name=db_service_name)
        
        # Use SQLAlchemy to manage the connection
        engine = create_engine(f'oracle+cx_oracle://{db_user}:{db_password}@{dsn}')

        # Define complex query with joins and subqueries
        query = """
        SELECT
            t1.transaction_id,
            t1.customer_id,
            t1.product_id,
            t1.amount,
            t2.total_quantity,
            (t1.amount / t2.total_quantity) AS avg_amount,
            t3.customer_name,
            t3.customer_email,
            t1.transaction_date
        FROM
            source_table t1
        JOIN
            (SELECT
                product_id,
                SUM(quantity) AS total_quantity
             FROM
                product_sales
             GROUP BY
                product_id) t2
        ON t1.product_id = t2.product_id
        JOIN
            customers t3
        ON t1.customer_id = t3.customer_id
        WHERE
            t1.transaction_date BETWEEN TO_DATE('2023-01-01', 'YYYY-MM-DD') AND TO_DATE('2023-12-31', 'YYYY-MM-DD')
        """

        # Extract data from the database
        logging.info("Extracting data from the database...")
        df = pd.read_sql(query, engine)
        
        # Save extracted data to a CSV file
        file_path = '/path/to/bronze/raw_data.csv'
        df.to_csv(file_path, index=False)
        logging.info(f"Data extracted and saved to {file_path}")

        return file_path

    except SQLAlchemyError as e:
        logging.error(f"Error while extracting data: {e}")
        raise

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise

if __name__ == "__main__":
    extract_data()
