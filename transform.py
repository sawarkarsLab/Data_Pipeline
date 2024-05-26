from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def transform_data():
    spark = SparkSession.builder.appName('TransformData').getOrCreate()
    df = spark.read.csv('/path/to/bronze/raw_data.csv', header=True)

    # Example transformation: Convert 'amount' column to float
    df = df.withColumn('amount', col('amount').cast('float'))
    
    df.write.csv('/path/to/silver/transformed_data.csv', header=True)
    return '/path/to/silver/transformed_data.csv'
