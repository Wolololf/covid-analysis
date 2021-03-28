import configparser
from pyspark.sql import SparkSession

from clean import clean_covid_data

def create_spark_session():
    """
    Creates the spark session
    """

    config = configparser.ConfigParser()
    config.read('dl.cfg')
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark

def process_covid_data():
    '''
    Comment

    Parameters:
    param_name (param_type): Description
    '''
    
    covid_cases_df = clean_covid_data("data/covid_cases_US.csv")
    covid_deaths_df = clean_covid_data("data/covid_deaths_US.csv")
    
    # Create one dataframe for counties, with fips, state and county name
    # Create another dataframe with fips as the first column, date as the second, and then cases and deaths as third and fourth columns
    # Might be a better way to do this in spark using temporary database views?