import pandas as pd
import logging
import numpy as np

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def validate_data():
    try:
        # Read transformed data
        df = pd.read_csv('/path/to/silver/transformed_data.csv')
        logging.info("Transformed data read into DataFrame.")

        # Define validation checks
        def check_nulls(df, column_name):
            if df[column_name].isnull().any():
                raise ValueError(f"Null values found in column: {column_name}")

        def check_duplicates(df, column_name):
            if df.duplicated(subset=[column_name]).any():
                raise ValueError(f"Duplicate values found in primary key column: {column_name}")

        def check_negative_values(df, column_name):
            if (df[column_name] < 0).any():
                raise ValueError(f"Negative values found in column: {column_name}")

        def check_column_values(df, column_name, valid_values):
            if not df[column_name].isin(valid_values).all():
                raise ValueError(f"Invalid values found in column: {column_name}")

        def check_date_format(df, column_name):
            try:
                pd.to_datetime(df[column_name])
            except ValueError:
                raise ValueError(f"Invalid date format in column: {column_name}")

        def check_join_integrity(df, column_name):
            if df[column_name].isnull().any():
                raise ValueError(f"Join integrity check failed: null values found in column: {column_name}")

        # Apply validation checks
        check_nulls(df, 'date')
        check_nulls(df, 'customer_id')
        check_duplicates(df, 'id')
        check_negative_values(df, 'amount')
        check_column_values(df, 'amount_category', ['Low', 'Medium', 'High'])
        check_date_format(df, 'date')
        check_join_integrity(df, 'product_id')

        # Check for outliers using Z-score
        df['amount_zscore'] = (df['amount'] - df['amount'].mean()) / df['amount'].std()
        if (df['amount_zscore'].abs() > 3).any():
            raise ValueError("Outliers detected in amount column")

        # Mask PII data in 'pii_column'
        df['pii_column'] = df['pii_column'].apply(lambda x: '***' + str(x)[-4:])
        logging.info("PII data masked.")

        # Ensure no null values in critical columns after transformation
        critical_columns = ['amount', 'amount_category', 'moving_avg', 'running_total']
        for col in critical_columns:
            check_nulls(df, col)

        # Save validated data
        file_path = '/path/to/gold/validated_data.csv'
        df.to_csv(file_path, index=False)
        logging.info(f"Validated data saved to {file_path}")

        return file_path

    except Exception as e:
        logging.error(f"Error during validation: {e}")
        raise

if __name__ == "__main__":
    validate_data()
