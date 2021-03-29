import pandas as pd

def clean_covid_data(spark, inputPath):
    '''
    Comment

    Parameters:
    spark (SparkContext): Spark context to run operations on
    inputPath (string): Path to the covid data file
    '''
    
    # TODO: Redo with spark!
    df = pd.read_csv(inputPath)
    
    # Select relevant columns
    county_columns = ["FIPS", "Admin2", "Province_State", "Lat", "Long_"]
    columns = county_columns + list(df.columns[11:])
    df = df[columns]
    
    # Rename columns for ease of use
    new_column_names = ["fips", "county_name", "state", "latitude", "longitude"]
    df = df.rename(columns=dict(zip(county_columns, new_column_names)))
    
    # Drop prisons
    df = df.drop([1304, 1336])
    
    # Drop fake counties (created for accounting purposes?)
    df = df.drop([1267, 2954, 2959, 2978, 2979, 2982, 2990])
    
    # Drop Kansas City since it's an aggregate of multiple counties, but doesn't have a FIPS code itself
    df = df.drop([1591])
    
    # Drop counties below 100 and above 60000 (overseas territories, cruise ships, out-of-state and unassigned counts)
    df = df.drop(df[(df["fips"] < 100) | (df["fips"] > 60000)].index)
    
    # Drop counties without any data
    df.drop(df[((df["state"] == "Utah") | (df["state"] == "Massachusetts")) & (df["2/27/21"] == 0)].index)
    
    # Fix the FIPS codes to 5 digits, as a string
    df.loc[df["fips"].notna(), "fips"] = df.loc[df["fips"].notna(), "fips"].astype(int).astype(str).str.pad(width=5, side='left', fillchar='0')

    # Force health data columns to numeric format. The first few columns (fips, county and state) don't need to be numeric.
    numeric_columns = df.columns[3:]
    df[numeric_columns] = df[numeric_columns].astype(int)
    
    return df

def clean_health_data(spark):
    '''
    Comment

    Parameters:
    spark (SparkContext): Spark context to run operations on
    '''
    
    # TODO: Redo with spark!
    health_df = pd.read_csv("data/health_data.csv")
    
    # Drop first row since it's just another set of column names
    health_df = health_df.drop(health_df.index[0])
    
    # Select relevant columns
    health_columns = ["5-digit FIPS Code", "Name", "State Abbreviation", "Population raw value", "Poor or fair health raw value", "Adult smoking raw value", "Adult obesity raw value", "Physical inactivity raw value", "Excessive drinking raw value", "Uninsured raw value", "Primary care physicians raw value", "Unemployment raw value", "Air pollution - particulate matter raw value", "Severe housing problems raw value", "Percentage of households with overcrowding", "Food insecurity raw value", "Residential segregation - non-White/White raw value", "% 65 and older raw value", "% Rural raw value"]
    health_df = health_df[health_columns]
    
    # Rename columns for ease of use
    new_column_names = ["fips", "county_name", "state", "population", "poor_health", "smokers", "obesity", "physical_inactivity", "excessive_drinking", "uninsured", "physicians", "unemployment", "air_pollution", "housing_problems", "household_overcrowding", "food_insecurity", "residential_segregation", "over_sixtyfives", "rural"]
    health_df = health_df.rename(columns=dict(zip(health_columns, new_column_names)))

    # Force health data columns to numeric format. The first few columns (fips, county and state) don't need to be numeric.
    numeric_columns = new_column_names[3:]
    health_df[numeric_columns] = health_df[numeric_columns].apply(pd.to_numeric)
    
    return health_df


def clean_area_data(spark):
    '''
    Comment

    Parameters:
    spark (SparkContext): Spark context to run operations on
    '''
    
    # TODO: Redo with spark!
    raw_area_df = pd.read_json("data/us_county_area.json")
    
    county_area_dict = {}
    county_area_dict = {county['properties']['GEO_ID'][-5:]: county['properties']['CENSUSAREA'] for county in raw_area_df['features']}

    county_area_df = pd.DataFrame(county_area_dict.items(), columns=["fips", "area"])
    
    # Potentially drop FIPS over 60000? Doesn't really matter, though, we'll just never use these if they don't exist in the Covid-19 data set.


def clean_weather_data(spark, inputPath):
    '''
    Comment

    Parameters:
    spark (SparkContext): Spark context to run operations on
    inputPath (string): Path to the weather data file
    '''
    
    # TODO: Redo with spark!
    df = pd.read_csv(inputPath)
    
    # Select relevant columns
    county_columns = ["FIPS", "Admin2", "Province_State", "Lat", "Long_"]
    columns = county_columns + list(df.columns[11:])
    df = df[columns]
    
    # Rename columns for ease of use
    new_column_names = ["fips", "county_name", "state", "latitude", "longitude"]
    df = df.rename(columns=dict(zip(county_columns, new_column_names)))
    
    # Drop counties below 100 and above 60000
    df = df.drop(df[(df["fips"] < 100) | (df["fips"] > 60000)].index)
    
    # Drop the broken weather station
    df = df.drop(85)
    
    # Fix the FIPS codes to 5 digits, as a string
    df.loc[df["fips"].notna(), "fips"] = df.loc[df["fips"].notna(), "fips"].astype(int).astype(str).str.pad(width=5, side='left', fillchar='0')

    # Force health data columns to numeric format. The first few columns (fips, county and state) don't need to be numeric.
    numeric_columns = df.columns[3:]
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric)
    
    return df


def clean_all_weather_data(spark):
    '''
    Read, clean and return all weather data

    Parameters::
    spark (SparkContext): Spark context to run operations on
    '''
    
    t_min_df = clean_weather_data(spark, "data/weather/tMin_US.csv")
    t_max_df = clean_weather_data(spark, "data/weather/tMax_US.csv")
    cloud_df = clean_weather_data(spark, "data/weather/cloud_US.csv")
    wind_df = clean_weather_data(spark, "data/weather/wind_US.csv")
    
    return t_min_df, t_max_df, cloud_df, wind_df