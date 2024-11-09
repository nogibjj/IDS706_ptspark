# IDS706_pyspark
![CI](https://github.com/nogibjj/IDS706_pyspark/actions/workflows/CICD.yml/badge.svg)

## Requirement
- Use PySpark to perform data processing on a large dataset
- Include at least one Spark SQL query and one data transformation

## Overview

This project uses PySpark to handle and process large datasets efficiently. The workflow includes:

1. Downloading a Parquet file and converting it to CSV.
2. Loading the CSV file into a Spark DataFrame.
3. Transforming data by calculating trip durations and average speeds.
4. Filtering and binning data based on speed.
5. Executing SQL queries on the transformed data for analysis.

## Features

- **Data Extraction**: Download a dataset from a URL in Parquet format and convert it to CSV.
- **Data Loading**: Load the CSV data into a Spark DataFrame.
- **Data Transformation**: Calculate trip duration and average speed, filter out invalid data, and create speed bins.
- **SQL Querying**: Perform SQL queries to analyze trip data, aggregating by passenger count and speed.

## Setup and Running the Code
### 1.  Setup the Repository:

```bash
# Copy code
git clone https://github.com/nogibjj/IDS706_pyspark.git
cd IDS706_pyspark
make install
# make sure you have java setup before run the script
```

### 2.  Run the Script: The script main.py will execute the entire workflow from downloading the data to querying it.

```bash
python main.py
```

## Explanation of Functions

The main script contains several functions that perform different stages of data processing:

### 1. `extract`

This function downloads the Parquet file(300MB+) from a specified URL, reads it into a Pandas DataFrame, and saves it as a CSV file. This step is necessary as the Parquet format is commonly used for large datasets but might not be directly readable without conversion.

```python
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
```

### 2. `start_spark`

This function starts a Spark session, which is required to use PySpark for data processing. Spark sessions enable you to create DataFrames and run SQL queries on large datasets efficiently.

```python
def start_spark(appName="TripDataAnalysis"):
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark
```

### 3. `load`

This function loads the CSV file created in the `extract` step into a PySpark DataFrame, which is a distributed data structure optimized for processing large datasets.

```python
def load(spark, file_path):
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df
```

### 4. `transform`

This function performs several transformations on the loaded DataFrame:

- **Calculate Trip Duration**: Computes the time difference between pickup and drop-off times in minutes.
- **Calculate Average Speed**: Calculates average speed (miles per hour) based on trip distance and duration.
- **Filter Data**: Removes entries with invalid trip distances or speeds.
- **Bin Speed**: Categorizes the data into speed bins (e.g., 0-10 mph, 10-20 mph) for easier analysis.

```python
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
```

### 5. `query`

The `query` function uses Spark SQL to perform an aggregate analysis of the transformed data. It groups trips by `passenger_count` and `speed_bin`, counting the number of trips and calculating average fares and total tips. The function outputs the top 5 combinations by trip count.

```python
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
```

## Output

The script’s output includes the top 5 combinations of passenger count and speed bin, with their respective statistics, displayed in the console. The output format will resemble:

```
+---------------+---------+-----------+------------------+------------------+   
|passenger_count|speed_bin|total_trips|   avg_fare_amount|  total_tip_amount|
+---------------+---------+-----------+------------------+------------------+
|            1.0|        0|    1163959|12.563343519831738|3009346.0799995624|
|            1.0|       10|     781951|16.971724327994853|2601164.8599997573|
|            2.0|        0|     205563|12.719062866371836| 530623.2400000356|
|            1.0|       20|     146919| 41.44937012911894|1082193.3300000022|
|            2.0|       10|     144286| 18.71283513299983|514806.36000001593|
+---------------+---------+-----------+------------------+------------------+
```