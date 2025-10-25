#!/usr/bin/env python3
"""
EDA Script for US Airline Flight Routes and Fares 1993-2024
Using PySpark to handle large dataset with multiLine option
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd

def create_spark_session():
    """Create Spark session with appropriate configuration"""
    spark = SparkSession.builder \
        .appName("Aviation_EDA") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .master("local[*]") \
        .getOrCreate()
    
    # Set log level to reduce warnings
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def load_data(spark, file_path):
    """Load CSV data with multiLine option"""
    print("Loading data with multiLine option...")
    df = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("inferSchema", "true") \
        .csv(file_path)
    return df

def basic_info(df):
    """Get basic information about the dataset"""
    print("=" * 60)
    print("BASIC DATASET INFORMATION")
    print("=" * 60)
    
    # Row count
    total_rows = df.count()
    print(f"Total number of rows: {total_rows:,}")
    
    # Column information
    print(f"\nNumber of columns: {len(df.columns)}")
    print("\nColumn names and types:")
    df.printSchema()
    
    # Show first few rows
    print("\nFirst 5 rows:")
    df.show(5, truncate=False)
    
    return total_rows

def data_quality_check(df):
    """Check data quality - missing values, duplicates"""
    print("\n" + "=" * 60)
    print("DATA QUALITY ANALYSIS")
    print("=" * 60)
    
    # Missing values
    print("\nMissing values per column:")
    missing_data = []
    for col in df.columns:
        null_count = df.filter(col(col).isNull()).count()
        missing_data.append((col, null_count))
    
    missing_df = spark.createDataFrame(missing_data, ["Column", "Null_Count"])
    missing_df.show(len(df.columns))
    
    # Check for duplicates
    total_rows = df.count()
    distinct_rows = df.distinct().count()
    duplicates = total_rows - distinct_rows
    print(f"\nDuplicate rows: {duplicates:,} ({duplicates/total_rows*100:.2f}%)")
    
    return missing_data

def descriptive_statistics(df):
    """Generate descriptive statistics for numeric columns"""
    print("\n" + "=" * 60)
    print("DESCRIPTIVE STATISTICS")
    print("=" * 60)
    
    # Numeric columns
    numeric_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, (IntegerType, LongType, DoubleType, FloatType))]
    print(f"Numeric columns: {numeric_cols}")
    
    if numeric_cols:
        print("\nDescriptive statistics for numeric columns:")
        df.select(numeric_cols).describe().show()
    
    return numeric_cols

def temporal_analysis(df):
    """Analyze data by year and quarter"""
    print("\n" + "=" * 60)
    print("TEMPORAL ANALYSIS")
    print("=" * 60)
    
    # Year distribution
    print("\nData distribution by year:")
    year_dist = df.groupBy("Year").count().orderBy("Year")
    year_dist.show()
    
    # Quarter distribution
    print("\nData distribution by quarter:")
    quarter_dist = df.groupBy("quarter").count().orderBy("quarter")
    quarter_dist.show()
    
    # Year-Quarter combination
    print("\nData distribution by Year-Quarter:")
    year_quarter_dist = df.groupBy("Year", "quarter").count().orderBy("Year", "quarter")
    year_quarter_dist.show(20)

def route_analysis(df):
    """Analyze flight routes and cities"""
    print("\n" + "=" * 60)
    print("ROUTE ANALYSIS")
    print("=" * 60)
    
    # Top cities by departure
    print("\nTop 10 departure cities:")
    top_departure = df.groupBy("city1").count().orderBy(desc("count")).limit(10)
    top_departure.show()
    
    # Top cities by arrival
    print("\nTop 10 arrival cities:")
    top_arrival = df.groupBy("city2").count().orderBy(desc("count")).limit(10)
    top_arrival.show()
    
    # Top routes
    print("\nTop 10 routes (city1 -> city2):")
    top_routes = df.groupBy("city1", "city2").count().orderBy(desc("count")).limit(10)
    top_routes.show()

def fare_analysis(df):
    """Analyze fare data"""
    print("\n" + "=" * 60)
    print("FARE ANALYSIS")
    print("=" * 60)
    
    # Basic fare statistics
    print("\nFare statistics:")
    df.select("fare").describe().show()
    
    # Fare by year
    print("\nAverage fare by year:")
    fare_by_year = df.groupBy("Year").agg(
        avg("fare").alias("avg_fare"),
        min("fare").alias("min_fare"),
        max("fare").alias("max_fare"),
        count("fare").alias("count")
    ).orderBy("Year")
    fare_by_year.show()
    
    # Fare by quarter
    print("\nAverage fare by quarter:")
    fare_by_quarter = df.groupBy("quarter").agg(
        avg("fare").alias("avg_fare"),
        min("fare").alias("min_fare"),
        max("fare").alias("max_fare")
    ).orderBy("quarter")
    fare_by_quarter.show()

def passenger_analysis(df):
    """Analyze passenger data"""
    print("\n" + "=" * 60)
    print("PASSENGER ANALYSIS")
    print("=" * 60)
    
    # Passenger statistics
    print("\nPassenger statistics:")
    df.select("passengers").describe().show()
    
    # Total passengers by year
    print("\nTotal passengers by year:")
    passengers_by_year = df.groupBy("Year").agg(
        sum("passengers").alias("total_passengers"),
        avg("passengers").alias("avg_passengers_per_route"),
        count("passengers").alias("route_count")
    ).orderBy("Year")
    passengers_by_year.show()

def carrier_analysis(df):
    """Analyze airline carriers"""
    print("\n" + "=" * 60)
    print("CARRIER ANALYSIS")
    print("=" * 60)
    
    # Large carriers
    print("\nTop large carriers by route count:")
    large_carriers = df.filter(col("carrier_lg").isNotNull()).groupBy("carrier_lg").count().orderBy(desc("count")).limit(10)
    large_carriers.show()
    
    # Low-cost carriers
    print("\nTop low-cost carriers by route count:")
    low_carriers = df.filter(col("carrier_low").isNotNull()).groupBy("carrier_low").count().orderBy(desc("count")).limit(10)
    low_carriers.show()

def distance_analysis(df):
    """Analyze flight distances"""
    print("\n" + "=" * 60)
    print("DISTANCE ANALYSIS")
    print("=" * 60)
    
    # Distance statistics
    print("\nDistance (nsmiles) statistics:")
    df.select("nsmiles").describe().show()
    
    # Distance by year
    print("\nAverage distance by year:")
    distance_by_year = df.groupBy("Year").agg(
        avg("nsmiles").alias("avg_distance"),
        min("nsmiles").alias("min_distance"),
        max("nsmiles").alias("max_distance")
    ).orderBy("Year")
    distance_by_year.show()

def main():
    """Main EDA function"""
    print("Starting EDA for US Airline Flight Routes and Fares 1993-2024")
    print("=" * 80)
    
    # Create Spark session
    spark = create_spark_session()
    
    # Load data
    file_path = "/home/jovyan/data/US Airline Flight Routes and Fares 1993-2024.csv"
    df = load_data(spark, file_path)
    
    # Basic information
    total_rows = basic_info(df)
    
    # Data quality check
    missing_data = data_quality_check(df)
    
    # Descriptive statistics
    numeric_cols = descriptive_statistics(df)
    
    # Temporal analysis
    temporal_analysis(df)
    
    # Route analysis
    route_analysis(df)
    
    # Fare analysis
    fare_analysis(df)
    
    # Passenger analysis
    passenger_analysis(df)
    
    # Carrier analysis
    carrier_analysis(df)
    
    # Distance analysis
    distance_analysis(df)
    
    print("\n" + "=" * 80)
    print("EDA COMPLETED SUCCESSFULLY")
    print("=" * 80)
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
