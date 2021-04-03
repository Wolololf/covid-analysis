import pandas as pd
from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, from_unixtime
from pyspark.sql.types import *

from setup import *
from clean import *

# For now, just locally, later on maybe write this to S3 instead
output_path = "output/"

def create_time_dimension_table(spark):
    '''
    Creates date data for each day in 2020, stores it in parquet and then returns the Spark dataframe for further use
    
    Parameters:
    spark (SparkContext): Spark context to run operations on
    
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


def load_covid_case_data(spark):
    '''
    Load Covid-19 case data

    Parameters:
    spark (SparkContext): Spark context to run operations on
    '''
    
    covid_cases_df = spark.read.load("data/covid_cases_US.csv", format="csv", sep=",", inferSchema="true", header="true")
    
    return covid_cases_df


def load_covid_deaths_data(spark):
    '''
    Load Covid-19 deaths data

    Parameters:
    spark (SparkContext): Spark context to run operations on
    '''
    
    covid_deaths_df = spark.read.load("data/covid_deaths_US.csv", format="csv", sep=",", inferSchema="true", header="true")
    
    return covid_deaths_df


def load_health_data(spark):
    '''
    Comment

    Parameters:
    spark (SparkContext): Spark context to run operations on
    '''
    
    health_df = spark.read.load("data/health_data.csv", format="csv", sep=",", inferSchema="true", header="true")
    
    return health_df


def load_area_data(spark):
    '''
    Comment

    Parameters:
    spark (SparkContext): Spark context to run operations on
    '''
    
    area_df = spark.read.load("data/us_county_area.csv", format="csv", sep=",", inferSchema="true", header="true")
    
    return area_df


def load_weather_data(spark):
    '''
    Comment

    Parameters:
    spark (SparkContext): Spark context to run operations on
    '''
    
    tMin_df = spark.read.load("data/tMin_US.csv", format="csv", sep=",", inferSchema="true", header="true")
    tMax_df = spark.read.load("data/tMax_US.csv", format="csv", sep=",", inferSchema="true", header="true")
    cloud_df = spark.read.load("data/cloud_US.csv", format="csv", sep=",", inferSchema="true", header="true")
    wind_df = spark.read.load("data/wind_US.csv", format="csv", sep=",", inferSchema="true", header="true")
    
    return tMin_df, tMax_df, cloud_df, wind_df
    

def create_county_dimension_table(spark, covid_cases_df, health_df):
    '''
    Create county dimension table from covid case data, county health data and area survey data

    Parameters:
    spark (SparkContext): Spark context to run operations on
    '''
    
    county_columns = ["fips", "county_name", "state", "latitude", "longitude"]
    county_dim_df = covid_cases_df[county_columns]
    
    health_df = health_df.withColumnRenamed('county_name', 'cn')
    county_dim_df = county_dim_df.join(health_df, on=["fips"], how="inner").drop('cn')
    
    area_df = load_area_data(spark)
    
    county_dim_df = county_dim_df.join(county_area_df, on=["fips"], how="inner")
    
    county_dim_df = county_dim_df.withColumn('population_density', county_dim_df['population'] / county_dim_df['area'])
    
    county_dim_df.write.partitionBy('state').mode('overwrite').parquet(output_path + "county_dim.parquet")
    
    return county_dim_df
    

def load_county_dimension_table(spark):
    '''
    Load county dimension table from parquet

    Parameters:
    spark (SparkContext): Spark context to run operations on
    '''
    
    county_dim_df = spark.read.parquet(output_path + "county_dim.parquet")
    
    return county_dim_df
    

def create_state_dimension_table(spark, health_df, county_dim_df):
    '''
    Create state dimension table from health data and some additional information about state area sizes from the county dimension table

    Parameters:
    spark (SparkContext): Spark context to run operations on
    '''
    
    state_dim_df = health_df.where((health_df['fips'] != 0) & (health_df['fips'] % 1000 == 0)).drop('fips').withColumnRenamed('county_name', 'state_name')
    
    state_area_df = county_dim_df.groupBy('state') \
        .agg(F.sum('area') \
        .alias('area'))
    
    state_dim_df = state_dim_df.join(state_area_df, on=["state"], how="inner").select(state_dim_df["*"], state_area_df["area"])
    
    state_dim_df = state_dim_df.withColumn('population_density', state_dim_df['population'] / state_dim_df['area'])
    
    state_dim_df.write.mode('overwrite').parquet(output_path + "state_dim.parquet")
    
    return state_dim_df
    

def load_state_dimension_table(spark):
    '''
    Load state dimension table from parquet

    Parameters:
    spark (SparkContext): Spark context to run operations on
    '''
    
    state_dim_df = spark.read.parquet(output_path + "state_dim.parquet")
    
    return state_dim_df
    

def create_county_facts_table(spark, covid_cases_df, covid_deaths_df):
    '''
    Comment

    Parameters:
    spark (SparkContext): Spark context to run operations on
    '''
    
    tMin_df, tMax_df, cloud_df, wind_df = load_weather_data(spark)


def create_state_facts_table(spark, covid_cases_df, covid_deaths_df):
    '''
    Comment

    Parameters:
    spark (SparkContext): Spark context to run operations on
    '''


def main():
    '''
    Runs the ETL pipeline.
    - 
    '''
    
    spark = create_spark_session()
    
    covid_cases_df = load_covid_case_data(spark)
    covid_deaths_df = load_covid_deaths_data(spark)
    
    health_df = load_health_data(spark)
    
    time_dim_df = create_time_dimension_table(spark)
    
    county_dim_df = create_county_dimension_table(spark, covid_cases_df, health_df)
    
    state_dim_df = create_state_dimension_table(spark, health_df, county_dim_dfs)
    
    county_facts_df = create_county_facts_table(spark)
    
    state_facts_df = create_state_facts_table(spark)


if __name__ == "__main__":
    main()