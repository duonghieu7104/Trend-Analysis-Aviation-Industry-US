#!/usr/bin/env python3
import os
import sys

# Set environment variables
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    
    print("Creating Spark session...")
    spark = SparkSession.builder \
        .appName("TestSpark") \
        .master("local[1]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()
    
    print("Spark session created successfully!")
    
    # Test reading a small sample
    print("Testing data loading...")
    df = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("inferSchema", "true") \
        .csv("/home/jovyan/data/US Airline Flight Routes and Fares 1993-2024.csv")
    
    print(f"Data loaded successfully! Rows: {df.count()}")
    print("Columns:", df.columns)
    df.show(5)
    
    spark.stop()
    print("Test completed successfully!")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
