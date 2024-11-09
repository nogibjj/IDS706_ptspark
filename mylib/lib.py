from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, when, floor
import requests
import pandas as pd
from io import BytesIO

def extract(parquet_url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet", output_csv_path="yellow_tripdata_2024-01.csv"):
    response = requests.get(parquet_url, stream=True)
    if response.status_code == 200:
        parquet_data = BytesIO(response.content)
        print("Parquet file downloaded successfully.")
    else:
        print("Failed to download the Parquet file.")
        return

    # Read the Parquet file with Pandas
    df = pd.read_parquet(parquet_data)

    # Save DataFrame as CSV
    df.to_csv(output_csv_path, index=False)
    print(f"Data successfully saved as CSV at {output_csv_path}")
    

def start_spark(appName="TripDataAnalysis"):
    """Start a Spark session"""
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark

def load(spark, file_path):
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df

def transform(df):
    df_with_duration = df.withColumn(
        'trip_duration',
        (unix_timestamp('tpep_dropoff_datetime') - unix_timestamp('tpep_pickup_datetime')) / 60
    )
    
    df_with_speed = df_with_duration.withColumn(
        'average_speed',
        when(col('trip_duration') > 0,
             col('trip_distance') / (col('trip_duration') / 60)
        ).otherwise(None)
    )
    
    df_filtered = df_with_speed.filter(
        (col('trip_distance') > 0) &
        (col('trip_duration') > 0) &
        (col('average_speed') >= 1) &
        (col('average_speed') <= 100)
    )
    
    # Step 4: Create Speed Bins (e.g., 0-10 mph, 10-20 mph)
    df_binned = df_filtered.withColumn(
        'speed_bin',
        (floor(col('average_speed') / 10) * 10).cast('int')
    )
    
    return df_binned

def query(df, spark):
    df.createOrReplaceTempView("taxi_trips")

    query = """
    SELECT
        passenger_count,
        speed_bin,
        COUNT(*) AS total_trips,
        AVG(fare_amount) AS avg_fare_amount,
        SUM(tip_amount) AS total_tip_amount
    FROM
        taxi_trips
    GROUP BY
        passenger_count,
        speed_bin
    ORDER BY
        total_trips DESC
    LIMIT 5
    """

    result = spark.sql(query)

    result.show()

if __name__ == "__main__":
    extract()
    
    spark = start_spark("TripDataAnalysis")

    file_path = "yellow_tripdata_2024-01.csv"
    df = load(spark, file_path)

    df_transformed = transform(df)

    query(df_transformed, spark)

    spark.stop()
