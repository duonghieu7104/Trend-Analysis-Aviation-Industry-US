#!/usr/bin/env python3
"""
EDA Script for US Airline Flight Routes and Fares 1993-2024
Using Pandas to handle large dataset with chunking
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def load_data(file_path, chunk_size=10000):
    """Load CSV data in chunks to handle large dataset"""
    print("Loading data in chunks...")
    print("=" * 60)
    
    # First, let's check the file size and structure
    print("Reading first few rows to understand structure...")
    sample_df = pd.read_csv(file_path, nrows=5)
    print(f"Sample data shape: {sample_df.shape}")
    print(f"Columns: {list(sample_df.columns)}")
    print("\nFirst few rows:")
    print(sample_df.head())
    
    # Load data in chunks
    print(f"\nLoading full dataset in chunks of {chunk_size} rows...")
    chunks = []
    chunk_count = 0
    
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        chunks.append(chunk)
        chunk_count += 1
        if chunk_count % 10 == 0:
            print(f"Loaded {chunk_count * chunk_size:,} rows...")
    
    print(f"Combining {len(chunks)} chunks...")
    df = pd.concat(chunks, ignore_index=True)
    print(f"Final dataset shape: {df.shape}")
    
    return df

def basic_info(df):
    """Get basic information about the dataset"""
    print("\n" + "=" * 60)
    print("BASIC DATASET INFORMATION")
    print("=" * 60)
    
    print(f"Dataset shape: {df.shape}")
    print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    print("\nColumn information:")
    print(df.info())
    
    print("\nFirst 5 rows:")
    print(df.head())
    
    print("\nLast 5 rows:")
    print(df.tail())
    
    return df.shape

def data_quality_check(df):
    """Check data quality - missing values, duplicates"""
    print("\n" + "=" * 60)
    print("DATA QUALITY ANALYSIS")
    print("=" * 60)
    
    # Missing values
    print("\nMissing values per column:")
    missing_data = df.isnull().sum()
    missing_percent = (missing_data / len(df)) * 100
    
    missing_df = pd.DataFrame({
        'Column': missing_data.index,
        'Missing_Count': missing_data.values,
        'Missing_Percent': missing_percent.values
    }).sort_values('Missing_Count', ascending=False)
    
    print(missing_df)
    
    # Check for duplicates
    total_rows = len(df)
    duplicate_rows = df.duplicated().sum()
    print(f"\nDuplicate rows: {duplicate_rows:,} ({duplicate_rows/total_rows*100:.2f}%)")
    
    # Check for completely empty rows
    empty_rows = df.isnull().all(axis=1).sum()
    print(f"Completely empty rows: {empty_rows:,}")
    
    return missing_df

def descriptive_statistics(df):
    """Generate descriptive statistics for numeric columns"""
    print("\n" + "=" * 60)
    print("DESCRIPTIVE STATISTICS")
    print("=" * 60)
    
    # Numeric columns
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    print(f"Numeric columns: {numeric_cols}")
    
    if numeric_cols:
        print("\nDescriptive statistics for numeric columns:")
        desc_stats = df[numeric_cols].describe()
        print(desc_stats)
        
        # Additional statistics
        print("\nAdditional statistics:")
        additional_stats = pd.DataFrame({
            'Column': numeric_cols,
            'Skewness': [df[col].skew() for col in numeric_cols],
            'Kurtosis': [df[col].kurtosis() for col in numeric_cols],
            'Zeros': [df[col].eq(0).sum() for col in numeric_cols],
            'Negatives': [df[col].lt(0).sum() for col in numeric_cols]
        })
        print(additional_stats)
    
    return numeric_cols

def temporal_analysis(df):
    """Analyze data by year and quarter"""
    print("\n" + "=" * 60)
    print("TEMPORAL ANALYSIS")
    print("=" * 60)
    
    # Year distribution
    print("\nData distribution by year:")
    year_dist = df['Year'].value_counts().sort_index()
    print(year_dist)
    
    # Quarter distribution
    print("\nData distribution by quarter:")
    quarter_dist = df['quarter'].value_counts().sort_index()
    print(quarter_dist)
    
    # Year-Quarter combination
    print("\nData distribution by Year-Quarter (top 20):")
    year_quarter_dist = df.groupby(['Year', 'quarter']).size().sort_values(ascending=False).head(20)
    print(year_quarter_dist)
    
    # Time range
    print(f"\nData time range: {df['Year'].min()} - {df['Year'].max()}")
    print(f"Total years covered: {df['Year'].max() - df['Year'].min() + 1}")

def route_analysis(df):
    """Analyze flight routes and cities"""
    print("\n" + "=" * 60)
    print("ROUTE ANALYSIS")
    print("=" * 60)
    
    # Top cities by departure
    print("\nTop 10 departure cities:")
    top_departure = df['city1'].value_counts().head(10)
    print(top_departure)
    
    # Top cities by arrival
    print("\nTop 10 arrival cities:")
    top_arrival = df['city2'].value_counts().head(10)
    print(top_arrival)
    
    # Top routes
    print("\nTop 10 routes (city1 -> city2):")
    top_routes = df.groupby(['city1', 'city2']).size().sort_values(ascending=False).head(10)
    print(top_routes)
    
    # Airport analysis
    print("\nTop 10 departure airports:")
    top_dep_airports = df['airport_1'].value_counts().head(10)
    print(top_dep_airports)
    
    print("\nTop 10 arrival airports:")
    top_arr_airports = df['airport_2'].value_counts().head(10)
    print(top_arr_airports)

def fare_analysis(df):
    """Analyze fare data"""
    print("\n" + "=" * 60)
    print("FARE ANALYSIS")
    print("=" * 60)
    
    # Basic fare statistics
    print("\nFare statistics:")
    fare_stats = df['fare'].describe()
    print(fare_stats)
    
    # Fare by year
    print("\nFare statistics by year:")
    fare_by_year = df.groupby('Year')['fare'].agg(['count', 'mean', 'median', 'std', 'min', 'max']).round(2)
    print(fare_by_year)
    
    # Fare by quarter
    print("\nFare statistics by quarter:")
    fare_by_quarter = df.groupby('quarter')['fare'].agg(['count', 'mean', 'median', 'std', 'min', 'max']).round(2)
    print(fare_by_quarter)
    
    # Fare distribution
    print(f"\nFare range: ${df['fare'].min():.2f} - ${df['fare'].max():.2f}")
    print(f"Fare quartiles:")
    print(df['fare'].quantile([0.25, 0.5, 0.75, 0.9, 0.95, 0.99]))

def passenger_analysis(df):
    """Analyze passenger data"""
    print("\n" + "=" * 60)
    print("PASSENGER ANALYSIS")
    print("=" * 60)
    
    # Passenger statistics
    print("\nPassenger statistics:")
    passenger_stats = df['passengers'].describe()
    print(passenger_stats)
    
    # Total passengers by year
    print("\nTotal passengers by year:")
    passengers_by_year = df.groupby('Year')['passengers'].agg(['sum', 'mean', 'count']).round(2)
    print(passengers_by_year)
    
    # Passenger distribution
    print(f"\nPassenger range: {df['passengers'].min():,} - {df['passengers'].max():,}")
    print(f"Passenger quartiles:")
    print(df['passengers'].quantile([0.25, 0.5, 0.75, 0.9, 0.95, 0.99]))

def carrier_analysis(df):
    """Analyze airline carriers"""
    print("\n" + "=" * 60)
    print("CARRIER ANALYSIS")
    print("=" * 60)
    
    # Large carriers
    print("\nTop large carriers by route count:")
    large_carriers = df['carrier_lg'].value_counts().head(10)
    print(large_carriers)
    
    # Low-cost carriers
    print("\nTop low-cost carriers by route count:")
    low_carriers = df['carrier_low'].value_counts().head(10)
    print(low_carriers)
    
    # Carrier market share analysis
    print("\nLarge carrier market share (by routes):")
    lg_market_share = df['carrier_lg'].value_counts(normalize=True).head(10) * 100
    print(lg_market_share.round(2))
    
    print("\nLow-cost carrier market share (by routes):")
    low_market_share = df['carrier_low'].value_counts(normalize=True).head(10) * 100
    print(low_market_share.round(2))

def distance_analysis(df):
    """Analyze flight distances"""
    print("\n" + "=" * 60)
    print("DISTANCE ANALYSIS")
    print("=" * 60)
    
    # Distance statistics
    print("\nDistance (nsmiles) statistics:")
    distance_stats = df['nsmiles'].describe()
    print(distance_stats)
    
    # Distance by year
    print("\nAverage distance by year:")
    distance_by_year = df.groupby('Year')['nsmiles'].agg(['mean', 'median', 'std', 'min', 'max']).round(2)
    print(distance_by_year)
    
    # Distance distribution
    print(f"\nDistance range: {df['nsmiles'].min():,} - {df['nsmiles'].max():,} miles")
    print(f"Distance quartiles:")
    print(df['nsmiles'].quantile([0.25, 0.5, 0.75, 0.9, 0.95, 0.99]))

def correlation_analysis(df, numeric_cols):
    """Analyze correlations between numeric variables"""
    print("\n" + "=" * 60)
    print("CORRELATION ANALYSIS")
    print("=" * 60)
    
    if len(numeric_cols) > 1:
        print("\nCorrelation matrix:")
        corr_matrix = df[numeric_cols].corr()
        print(corr_matrix.round(3))
        
        # Find high correlations
        print("\nHigh correlations (|r| > 0.7):")
        high_corr = []
        for i in range(len(corr_matrix.columns)):
            for j in range(i+1, len(corr_matrix.columns)):
                corr_val = corr_matrix.iloc[i, j]
                if abs(corr_val) > 0.7:
                    high_corr.append((corr_matrix.columns[i], corr_matrix.columns[j], corr_val))
        
        if high_corr:
            for var1, var2, corr in high_corr:
                print(f"{var1} - {var2}: {corr:.3f}")
        else:
            print("No high correlations found.")

def outlier_analysis(df, numeric_cols):
    """Analyze outliers in numeric columns"""
    print("\n" + "=" * 60)
    print("OUTLIER ANALYSIS")
    print("=" * 60)
    
    for col in numeric_cols:
        if col in df.columns:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
            outlier_percent = (len(outliers) / len(df)) * 100
            
            print(f"\n{col}:")
            print(f"  Outliers: {len(outliers):,} ({outlier_percent:.2f}%)")
            print(f"  Range: {df[col].min():.2f} - {df[col].max():.2f}")
            print(f"  IQR bounds: {lower_bound:.2f} - {upper_bound:.2f}")

def main():
    """Main EDA function"""
    print("Starting EDA for US Airline Flight Routes and Fares 1993-2024")
    print("Using Pandas for data analysis")
    print("=" * 80)
    
    # Load data
    file_path = "/home/jovyan/data/US Airline Flight Routes and Fares 1993-2024.csv"
    df = load_data(file_path)
    
    # Basic information
    shape = basic_info(df)
    
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
    
    # Correlation analysis
    correlation_analysis(df, numeric_cols)
    
    # Outlier analysis
    outlier_analysis(df, numeric_cols)
    
    print("\n" + "=" * 80)
    print("EDA COMPLETED SUCCESSFULLY")
    print("=" * 80)
    
    # Summary
    print("\nSUMMARY:")
    print(f"- Dataset contains {shape[0]:,} rows and {shape[1]} columns")
    print(f"- Time period: {df['Year'].min()} - {df['Year'].max()}")
    print(f"- Total unique routes: {df.groupby(['city1', 'city2']).ngroups:,}")
    print(f"- Total unique airports: {pd.concat([df['airport_1'], df['airport_2']]).nunique():,}")
    print(f"- Average fare: ${df['fare'].mean():.2f}")
    print(f"- Total passengers: {df['passengers'].sum():,}")

if __name__ == "__main__":
    main()
