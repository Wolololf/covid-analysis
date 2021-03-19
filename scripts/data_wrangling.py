import pandas as pd
from tqdm import tqdm

def clean_and_combine_covid_data():
    '''
    Comment

    Parameters:
    param_name (param_type): Description
    '''
    
    covid_cases_df = pd.read_csv("data/covid_cases_US.csv")
    covid_deaths_df = pd.read_csv("data/covid_deaths_US.csv")
    
    # Fix up missing county name by using province name
    
    # Drop first few columns
    
    # Rename "FIPS" to "fips", "Admin2" to "county_name", "Province_State" to "state", "lat" to "latitude", "Long_" to "longitude"
    
    # I probably have to keep the date columns with the cases/deaths like this for now.
    # During ETL, when I read in each row, I can use the date to make an entry in the database for fips+date combo and store cases.
    # I can then find the same fips+date key to add my deaths to, or make a new entry if needed.