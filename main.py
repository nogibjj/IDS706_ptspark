from mylib.lib import extract, start_spark, load, transform, query

if __name__ == "__main__":
    extract()
    
    spark = start_spark("TripDataAnalysis")

    file_path = "yellow_tripdata_2024-01.csv"
    df = load(spark, file_path)

    df_transformed = transform(df)

    query(df_transformed, spark)

    spark.stop()