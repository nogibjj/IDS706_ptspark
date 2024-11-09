import unittest
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from io import BytesIO
import requests
import pandas as pd
import os

from mylib.lib import extract, start_spark, load, transform, query

class TestTripDataAnalysis(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestTripDataAnalysis").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_extract(self):
        output_csv_path = "test_yellow_tripdata.csv"
        extract(output_csv_path=output_csv_path)
        
        self.assertTrue(os.path.exists(output_csv_path))
        
        df = pd.read_csv(output_csv_path)
        self.assertFalse(df.empty)
        
        os.remove(output_csv_path)

    def test_start_spark(self):
        spark = start_spark("TestSession")
        self.assertIsInstance(spark, SparkSession)

    def test_load(self):
        # Create a small sample CSV for loading test
        sample_csv = "test_sample.csv"
        pd.DataFrame({
            'tpep_pickup_datetime': ['2024-01-01 00:15:00'],
            'tpep_dropoff_datetime': ['2024-01-01 00:30:00'],
            'trip_distance': [5.0],
            'fare_amount': [15.0],
            'tip_amount': [3.0],
            'passenger_count': [2]
        }).to_csv(sample_csv, index=False)
        
        df = load(self.spark, sample_csv)
        self.assertIsInstance(df, DataFrame)
        
        self.assertIn('tpep_pickup_datetime', df.columns)
        
        os.remove(sample_csv)

    def test_transform(self):
        data = [
            ("2024-01-01 00:15:00", "2024-01-01 00:30:00", 5.0, 15.0, 3.0, 2),
            ("2024-01-01 00:10:00", "2024-01-01 00:20:00", 3.0, 10.0, 2.0, 1)
        ]
        columns = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount", "tip_amount", "passenger_count"]
        df = self.spark.createDataFrame(data, columns)
        
        df_transformed = transform(df)
        
        self.assertIn('trip_duration', df_transformed.columns)
        self.assertIn('average_speed', df_transformed.columns)
        self.assertIn('speed_bin', df_transformed.columns)
        
        trip_distances = [row['trip_distance'] for row in df_transformed.collect()]
        self.assertTrue(all(distance > 0 for distance in trip_distances))
      

if __name__ == "__main__":
    unittest.main()
