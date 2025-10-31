# US Airline Flight Routes and Fares Dataset (1993-2024)

## Overview

This dataset provides comprehensive information on airline flight routes, fares, and passenger volumes within the United States spanning from 1993 to 2024. The data encompasses detailed metrics including origin and destination cities, distances between airports, passenger counts, and fare information segmented by different airline carriers. This dataset serves as an invaluable resource for analyzing trends in air travel, pricing dynamics, and carrier competition over three decades.

## Dataset Information

- **File Name**: `US Airline Flight Routes and Fares 1993-2024.csv`
- **Time Period**: 1993 - 2024
- **Geographic Coverage**: United States domestic routes
- **Data Type**: Time series data with quarterly granularity

## Data Features

### Core Identifiers
- **tbl**: Table identifier
- **tbl1apk**: Unique identifier for the route

### Temporal Information
- **Year**: Year of the data record (1993-2024)
- **quarter**: Quarter of the year (1-4)

### Geographic Information
- **citymarketid_1**: Origin city market ID
- **citymarketid_2**: Destination city market ID
- **city1**: Origin city name
- **city2**: Destination city name
- **airportid_1**: Origin airport ID
- **airportid_2**: Destination airport ID
- **airport_1**: Origin airport code
- **airport_2**: Destination airport code
- **Geocoded_City1**: Geocoded coordinates for the origin city
- **Geocoded_City2**: Geocoded coordinates for the destination city

### Route Metrics
- **nsmiles**: Distance between airports in miles
- **passengers**: Number of passengers

### Pricing Information
- **fare**: Average fare
- **fare_lg**: Average fare of the largest carrier
- **fare_low**: Lowest fare

### Market Analysis
- **carrier_lg**: Code for the largest carrier by passengers
- **large_ms**: Market share of the largest carrier
- **carrier_low**: Code for the lowest fare carrier
- **lf_ms**: Market share of the lowest fare carrier

## Potential Use Cases

### 1. Market Analysis
- Assess trends in air travel demand over time
- Analyze fare changes and pricing patterns
- Evaluate market share evolution of airlines
- Study seasonal variations in passenger volumes

### 2. Price Optimization
- Develop predictive models for optimal pricing strategies
- Analyze price elasticity across different routes
- Identify pricing opportunities and competitive gaps
- Model fare sensitivity to market conditions

### 3. Route Planning
- Identify profitable routes and underserved markets
- Analyze route performance metrics
- Support strategic route planning decisions
- Evaluate route expansion opportunities

### 4. Economic Studies
- Analyze the economic impact of air travel on different cities and regions
- Study correlation between air connectivity and economic development
- Assess regional economic disparities in air travel access
- Evaluate infrastructure investment impacts

### 5. Travel Behavior Research
- Study changes in passenger preferences over the years
- Analyze travel behavior patterns and trends
- Investigate factors influencing route choice
- Examine demographic and economic influences on travel

### 6. Competitor Analysis
- Evaluate performance of different airlines on various routes
- Analyze competitive dynamics and market positioning
- Study carrier-specific strategies and outcomes
- Assess market concentration and competition levels

## Data Quality Considerations

- **Completeness**: The dataset spans 31 years with quarterly data points
- **Consistency**: Standardized airport codes and city identifiers
- **Accuracy**: Includes both passenger counts and fare information
- **Geographic Coverage**: Comprehensive coverage of US domestic routes

## Technical Notes

- Data is organized by route and time period
- Each record represents a specific route-quarter combination
- Market share calculations are based on passenger volumes
- Fare data includes both average and carrier-specific pricing
- Geographic coordinates enable spatial analysis capabilities

## Usage Recommendations

1. **Data Preprocessing**: Consider filtering for specific time periods or routes of interest
2. **Missing Values**: Check for any missing data points, especially in fare information
3. **Outliers**: Review passenger and fare data for potential outliers
4. **Aggregation**: Consider aggregating data by different dimensions (yearly, by carrier, by region)
5. **Validation**: Cross-reference with external data sources when possible

## Citation and Attribution

This dataset is intended for research and analysis purposes. Please ensure proper attribution when using this data in publications or reports.

---
