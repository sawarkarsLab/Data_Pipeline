from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf, avg, sum as spark_sum, row_number, monotonically_increasing_id
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType, IntegerType
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def transform_data():
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName('TransformData') \
            .config('spark.some.config.option', 'some-value') \
            .getOrCreate()

        logging.info("Spark session created.")

        # Read raw data
        df = spark.read.csv('/path/to/bronze/raw_data.csv', header=True, inferSchema=True)
        logging.info("Raw data read into DataFrame.")

        # Read lookup table
        product_df = spark.read.csv('/path/to/lookup/product_lookup.csv', header=True, inferSchema=True)
        logging.info("Product lookup data read into DataFrame.")

        # Define UDF to categorize amount
        def categorize_amount(amount):
            if amount < 100:
                return 'Low'
            elif 100 <= amount < 500:
                return 'Medium'
            else:
                return 'High'

        categorize_amount_udf = udf(categorize_amount, StringType())

        # Apply transformations
        df = df.withColumn('amount', col('amount').cast(DoubleType())) \
               .withColumn('amount_category', categorize_amount_udf(col('amount'))) \
               .withColumn('is_high_value', when(col('amount') > 500, 'Yes').otherwise('No'))

        logging.info("Transformations applied to DataFrame.")

        # Add a monotonically increasing ID to maintain order
        df = df.withColumn('id', monotonically_increasing_id())
        logging.info("Monotonically increasing ID added to DataFrame.")

        # Perform window function to rank transactions by amount for each customer
        window_spec = Window.partitionBy('customer_id').orderBy(col('amount').desc())
        df = df.withColumn('rank', row_number().over(window_spec))
        logging.info("Window function applied to rank transactions by amount for each customer.")

        # Join with product lookup table to enrich the data
        df = df.join(product_df, on='product_id', how='left')
        logging.info("Data joined with product lookup table.")

        # Perform complex aggregation
        agg_df = df.groupBy('customer_id').agg(
            avg('amount').alias('avg_amount'),
            spark_sum('total_quantity').alias('total_quantity'),
            spark_sum('amount').alias('total_spent'),
            spark_sum(when(col('is_high_value') == 'Yes', 1).otherwise(0)).alias('high_value_transactions')
        )
        logging.info("Complex aggregations computed.")

        # Join aggregated data back with the original DataFrame
        final_df = df.join(agg_df, on='customer_id', how='left')
        logging.info("Joined aggregated DataFrame with original DataFrame.")

        # Apply additional window functions: Running total and moving average
        running_total_window = Window.partitionBy('customer_id').orderBy('date').rowsBetween(Window.unboundedPreceding, Window.currentRow)
        moving_avg_window = Window.partitionBy('customer_id').orderBy('date').rowsBetween(-2, 2)

        final_df = final_df.withColumn('running_total', spark_sum('amount').over(running_total_window))
        final_df = final_df.withColumn('moving_avg', avg('amount').over(moving_avg_window))
        logging.info("Additional window functions (running total and moving average) applied.")

        # Filter out only the top-ranked transactions
        final_df = final_df.filter(col('rank') == 1)
        logging.info("Filtered to keep only the top-ranked transactions for each customer.")

        # Save transformed data
        file_path = '/path/to/silver/transformed_data.csv'
        final_df.write.csv(file_path, header=True)
        logging.info(f"Transformed data saved to {file_path}")

        return file_path

    except Exception as e:
        logging.error(f"Error during transformation: {e}")
        raise

if __name__ == "__main__":
    transform_data()
