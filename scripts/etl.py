import configparser
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, from_unixtime
from pyspark.sql.types import *

from clean import clean_covid_data

# For now, just locally, later on maybe write this to S3 instead
output_path = "output/"

def create_spark_session():
    """
    Creates the spark session
    
    Returns:
    Newly created spark session
    """

    config = configparser.ConfigParser()
    config.read('dl.cfg')

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()

    return spark

def process_time_data():
    '''
    Creates date data for each day in 2020, stores it in parquet and then returns the Spark dataframe for further use
    
    Returns:
    Spark dataframe for date
    '''
    
    time_df_pd = pd.DataFrame({'date':pd.date_range('2020-01-01', '2020-12-31')})
    time_df = spark.createDataFrame(time_df_pd)
    
    # Spark 3.0+ for some reason removed the ability to parse weekdays into integers, it only supports strings now.
    # Don't ask me why, I can't see how that's a good restriction to add.
    # We can fall back to the legacy time parser to restore the old behaviour.
    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

    time_df = time_df.withColumn('day', dayofmonth('date')) \
        .withColumn('week', weekofyear('date')) \
        .withColumn('month', month('date')) \
        .withColumn('year', year('date')) \
        .withColumn('weekday', date_format(col("date"), "u"))
    
    # Even though the original pandas dataframe used datetime, the spark dataframe reverted to timestamp.
    # I really don't need the time-of-day parts, so let's force this back to datetime.
    time_df = time_df.withColumn('date', time_df['date'].cast(DateType()))
    
    time_df.write.partitionBy('month').mode('overwrite').parquet(output_path + "time.parquet")
    
    return time_df

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