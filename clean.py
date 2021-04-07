import pandas as pd

def clean_covid_data(inputPath, outputPath):
    '''
    Clean Covid-19 data and write the cleaned data out to a new .csv file including a schema file.

    Parameters:
    inputPath (string): Path to the covid data input file
    outputPath (string): Path to the covid data output file
    '''
    
    print(f"Starting cleaning data at '{inputPath}'")
    
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
    numeric_columns = df.columns[5:]
    df[numeric_columns] = df[numeric_columns].astype(int)
    
    df.to_csv(outputPath, index=False)
    
    print(f"Wrote cleaned data from '{inputPath}' to '{outputPath}'")

    
def clean_health_data(inputPath, outputPath):
    '''
    Cleans and writes back to .csv the health data

    Parameters:
    inputPath (string): Path to the health data input file
    outputPath (string): Path to the health data output file
    '''
    
    print(f"Starting cleaning data at '{inputPath}'")
    
    health_df = pd.read_csv(inputPath) # data/health_data.csv
    
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
    
    health_df.to_csv(outputPath, index=False)


def clean_area_data(inputPath, outputPath):
    '''
    Cleans and writes back to .csv the area data

    Parameters:
    inputPath (string): Path to the health data input file
    outputPath (string): Path to the health data output file
    '''
    
    raw_area_df = pd.read_json(inputPath) # data/us_county_area.json
    
    county_area_dict = {}
    county_area_dict = {county['properties']['GEO_ID'][-5:]: county['properties']['CENSUSAREA'] for county in raw_area_df['features']}

    county_area_df = pd.DataFrame(county_area_dict.items(), columns=["fips", "area"])
    
    county_area_df.to_csv(outputPath, index=False)
    
    print(f"Wrote cleaned data from '{inputPath}' to '{outputPath}'")


def clean_weather_data(inputPath, outputPath):
    '''
    Cleans and writes back to .csv the weather data

    Parameters:
    inputPath (string): Path to the weather data input file
    outputPath (string): Path to the weather data output file
    '''
    
    print(f"Starting cleaning data at '{inputPath}'")
    
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
    
    df.to_csv(outputPath, index=False)
    
    print(f"Wrote cleaned data from '{inputPath}' to '{outputPath}'")

    
def clean_all_weather_data():
    '''
    Read, clean and write back to .csv all weather data
    '''
    
    clean_weather_data("raw_data/weather/tMin_US.csv", "data/tMin_US.csv")
    clean_weather_data("raw_data/weather/tMax_US.csv", "data/tMax_US.csv")
    clean_weather_data("raw_data/weather/cloud_US.csv", "data/cloud_US.csv")
    clean_weather_data("raw_data/weather/wind_US.csv", "data/wind_US.csv")
    

def main():
    '''
    Cleans all input data
    '''
    
    print("Starting clean step")
    
    clean_covid_data("raw_data/covid_cases_US.csv", "data/covid_cases_US.csv")
    clean_covid_data("raw_data/covid_deaths_US.csv", "data/covid_deaths_US.csv")
    
    clean_health_data("raw_data/health_data.csv", "data/health_data.csv")
    
    clean_area_data("raw_data/us_county_area.json", "data/us_county_area.csv")
    
    clean_all_weather_data()
    
    print("Finished cleaning data")


if __name__ == "__main__":
    main()