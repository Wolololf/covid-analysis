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
    time_df (Dataframe): Spark dataframe representing the time dimension table
    '''
    
    covid_cases_df = load_covid_case_data(spark)
    
    unix_time = pd.Timestamp("1970-01-01")
    second = pd.Timedelta('1s')

    date_list = [((pd.to_datetime(c) - unix_time) // second, ) for c in covid_cases_df.columns[5:]]
    
    time_columns = ['timestamp']
    time_df = spark.createDataFrame(date_list, time_columns)
    
    time_df = time_df.withColumn("date", F.from_unixtime("timestamp").cast(DateType()))
    
    # Spark 3.0+ for some reason removed the ability to parse weekdays into integers, it only supports strings now.
    # Don't ask me why, I can't see how that's a good restriction to add.
    # We can fall back to the legacy time parser to restore the old behaviour.
    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

    time_df = time_df.withColumn('day', dayofmonth('date')) \
        .withColumn('week', weekofyear('date')) \
        .withColumn('month', month('date')) \
        .withColumn('year', year('date')) \
        .withColumn('weekday', date_format(col("date"), "u").cast(IntegerType()))
    
    time_df = time_df.drop('date')
    
    time_df.write.partitionBy('month').mode('overwrite').parquet(output_path + "time.parquet")
    
    return time_df
    

def load_time_dimension_table(spark):
    '''
    Load time dimension table from parquet

    Parameters:
    spark (SparkContext): Spark context to run operations on
    
    Returns:
    time_df (Dataframe): Spark dataframe representing the time dimension table
    '''
    
    time_df = spark.read.parquet(output_path + "time.parquet")
    
    return time_df


def load_covid_case_data(spark):
    '''
    Load Covid-19 case data

    Parameters:
    spark (SparkContext): Spark context to run operations on
    
    Returns:
    covid_cases_df (Dataframe): Spark dataframe with the Covid-19 case data
    '''
    
    covid_cases_df = spark.read.load("data/covid_cases_US.csv", format="csv", sep=",", inferSchema="true", header="true")
    
    return covid_cases_df


def load_covid_deaths_data(spark):
    '''
    Load Covid-19 deaths data

    Parameters:
    spark (SparkContext): Spark context to run operations on
    
    Returns:
    covid_deaths_df (Dataframe): Spark dataframe with the Covid-19 death data
    '''
    
    covid_deaths_df = spark.read.load("data/covid_deaths_US.csv", format="csv", sep=",", inferSchema="true", header="true")
    
    return covid_deaths_df


def load_health_data(spark):
    '''
    Comment

    Parameters:
    spark (SparkContext): Spark context to run operations on
    
    Returns:
    health_df (Dataframe): Spark dataframe with the US county health data
    '''
    
    health_df = spark.read.load("data/health_data.csv", format="csv", sep=",", inferSchema="true", header="true")
    
    return health_df


def load_area_data(spark):
    '''
    Comment

    Parameters:
    spark (SparkContext): Spark context to run operations on
    
    Returns:
    area_df (Dataframe): Spark dataframe with the US county area data
    '''
    
    area_df = spark.read.load("data/us_county_area.csv", format="csv", sep=",", inferSchema="true", header="true")
    
    return area_df


def load_weather_data(spark):
    '''
    Comment

    Parameters:
    spark (SparkContext): Spark context to run operations on
    
    Returns:
    tMin_df (Dataframe): Spark dataframe with minimum temperature data per US county
    tMax_df (Dataframe): Spark dataframe with maximum temperature data per US county
    cloud_df (Dataframe): Spark dataframe with cloud cover data per US county
    wind_df (Dataframe): Spark dataframe with wind speeds data per US county
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
    
    Returns:
    county_dim_df (Dataframe): Spark dataframe representing the county dimension table
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
    
    Returns:
    county_dim_df (Dataframe): Spark dataframe representing the county dimension table
    '''
    
    county_dim_df = spark.read.parquet(output_path + "county_dim.parquet")
    
    return county_dim_df
    

def create_state_dimension_table(spark, health_df, county_dim_df):
    '''
    Create state dimension table from health data and some additional information about state area sizes from the county dimension table

    Parameters:
    spark (SparkContext): Spark context to run operations on
    health_df (DataFrame): Health data from a cleaning step or loaded from disc, provides all necessary data apart from area
    county_dim_df (DataFrame): County dimension data from a previous ETL step or loaded from disc, used to extract and accumulate area data
    
    Returns:
    state_dim_df (Dataframe): Spark dataframe representing the state dimension table
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
    
    Returns:
    state_dim_df (Dataframe): Spark dataframe representing the state dimension table
    '''
    
    state_dim_df = spark.read.parquet(output_path + "state_dim.parquet")
    
    return state_dim_df
    

def create_covid_time_series(spark, input_df, column_offset, total_column_name, delta_column_name, include_state):
    '''
    Create and return a time series of Covid-19 data by county

    Parameters:
    spark (SparkContext): Spark context to run operations on
    input_df (DataFrame): Source Covid-19 data, either from a previous cleaning step, or loaded from disc
    total_column_name (String): Column name for the total value (case or death) in each row
    delta_column_name (String): Column name for the delta value (case or death) in each row compared to the previous day
    include_state (Boolean): Do we want to include the state column in this dataframe?
    
    Returns:
    output_df (Dataframe): Spark dataframe containing a time series of Covid-19 data over time by county
    '''
    
    unix_time = pd.Timestamp("1970-01-01")
    second = pd.Timedelta('1s')
    
    date_list = [(pd.to_datetime(c) - unix_time) // second for c in input_df.columns[column_offset:]]

    time_data_columns = input_df.columns[column_offset:]
    time_data_columns.insert(0, 'fips')
        
    if include_state:
        time_data_columns.insert(1, 'state')

    time_series = []

    def extract_county_data_including_state(row):
        fips = time_data_columns[0]
        state = time_data_columns[1]
        for i in range(2, len(time_data_columns)):
            time_series.append((row[fips], row[state], date_list[i - 2], row[time_data_columns[i]]))

    def extract_county_data_excluding_state(row):
        fips = time_data_columns[0]
        for i in range(1, len(time_data_columns)):
            time_series.append((row[fips], row[state], date_list[i - 1], row[time_data_columns[i]]))

    
    if include_state:
        for row in input_df.collect():
            extract_county_data_including_state(row)
    else:
        for row in input_df.collect():
            extract_county_data_excluding_state(row)

    time_series_columns = ["fips", "timestamp", total_column_name]

    output_df = spark.createDataFrame(time_series, time_series_columns)

    windowSpec = Window \
        .partitionBy(county_deaths_df['fips']) \
        .orderBy(county_deaths_df['timestamp'].asc())

    output_df = output_df.withColumn('lag', F.lag(output_df[total_column_name], 1).over(windowSpec))
    output_df = output_df.withColumn('lead', F.lead(output_df[total_column_name], 1).over(windowSpec))

    # Populate deltas
    output_df = output_df.withColumn(delta_column_name, \
        F.when(output_df['lag'].isNull(), 0) \
        .otherwise(output_df[total_column_name] - output_df['lag']))

    output_df = output_df.withColumn('next_delta', F.lead(output_df[delta_column_name], 1).over(windowSpec))

    # Fix overreporting
    output_df = output_df.withColumn(total_column_name, \
        F.when((output_df['next_delta'] >= 0) | (output_df['lag'].isNull() | (output_df['lead'].isNull())), output_df[total_column_name]) \
        .otherwise(F.ceil((output_df['lead'] + output_df['lag']) / 2)))

    # Recalculate deltas
    output_df = output_df.withColumn('lag', F.lag(output_df[total_column_name], 1).over(windowSpec))
    output_df = output_df.withColumn(delta_column_name, \
        F.when(output_df['lag'].isNull(), 0) \
        .otherwise(output_df[total_column_name] - output_df['lag']))

    output_df = output_df.drop('lag').drop('lead').drop('next_delta')
    
    return output_df


def transform_weather_data(spark, input_df, column_name):
    '''
    Create and return a time series of weather data by county

    Parameters:
    spark (SparkContext): Spark context to run operations on
    input_df (DataFrame): Source weather data, either from a previous cleaning step, or loaded from disc
    column_name (String): Column name under which the weather data will appear
    
    Returns:
    output_df (Dataframe): Spark dataframe containing a time series of weather data over time by county
    '''
    
    unix_time = pd.Timestamp("1970-01-01")
    second = pd.Timedelta('1s')
    
    date_list = [(pd.to_datetime(c) - unix_time) // second for c in input_df.columns[5:]]

    time_data_columns = input_df.columns[5:]
    time_data_columns.insert(0, 'fips')

    time_series = []

    def extract_weather_data(row):
        fips = time_data_columns[0]
        for i in range(1, len(time_data_columns)):
            time_series.append((row[fips], date_list[i - 1], float(row[time_data_columns[i]])))

    for row in input_df.collect():
        extract_weather_data(row)

    time_series_columns = ["fips", "timestamp", column_name]

    output_df = spark.createDataFrame(time_series, time_series_columns)
    
    return output_df
    
    
def create_county_facts_table(spark, covid_cases_df, covid_deaths_df):
    '''
    Create the county facts table from Covid-19 case/death data and weather data

    Parameters:
    spark (SparkContext): Spark context to run operations on
    covid_cases_df (DataFrame): Covid-19 case data
    covid_deaths_df (DataFrame): Covid-19 death data
    
    Returns:
    facts_df (Dataframe): Spark dataframe representing the county facts table
    '''
    
    cases_df = create_covid_time_series(spark, covid_cases_df, 5, "covid_case_total", "covid_case_delta", True)
    deaths_df = create_covid_time_series(spark, covid_deaths_df, 6, "covid_death_total", "covid_death_delta", False)
    
    facts_df = cases_df.join(deaths_df, on=["fips", "timestamp"], how="left")
    
    tMin_df, tMax_df, cloud_df, wind_df = load_weather_data(spark)
    
    transformed_tMin_df = transform_weather_data(tMin_df, "t_min")
    facts_df = facts_df.join(transformed_tMin_df, on=["fips", "timestamp"], how="left")
    
    transformed_tMax_df = transform_weather_data(tMax_df, "t_max")
    facts_df = facts_df.join(transformed_tMax_df, on=["fips", "timestamp"], how="left")
    
    transformed_cloud_df = transform_weather_data(cloud_df, "cloud")
    facts_df = facts_df.join(transformed_cloud_df, on=["fips", "timestamp"], how="left")
    
    transformed_wind_df = transform_weather_data(wind_df, "wind")
    facts_df = facts_df.join(transformed_wind_df, on=["fips", "timestamp"], how="left")
    
    facts_df.write.partitionBy('fips').mode('append').parquet(output_path + "county_facts.parquet")
    
    return facts_df
    

def load_county_facts_table(spark):
    '''
    Load county facts table from parquet

    Parameters:
    spark (SparkContext): Spark context to run operations on
    
    Returns:
    county_facts_df (Dataframe): Spark dataframe representing the county facts table
    '''
    
    county_facts_df = spark.read.option("basePath", output_path + "county_facts.parquet").parquet(output_path + "county_facts.parquet")
    
    return county_facts_df


def create_state_facts_table(spark, county_facts_df):
    '''
    Create the state facts table based on aggregations over the county facts table

    Parameters:
    spark (SparkContext): Spark context to run operations on
    county_facts_df (DataFrame): County facts table that was created in a previous step (or loaded from disc)
    
    Returns:
    state_facts_df (Dataframe): Spark dataframe representing the state facts table
    '''
    
    county_facts_df_reduced = county_facts_df[['state', 'timestamp', 'covid_case_total', 'covid_case_delta', 'covid_death_total', 'covid_death_delta']]
    
    state_facts_df = county_facts_df_reduced.groupBy('state', 'timestamp').agg( \
        F.sum('covid_case_total').alias('covid_case_total'), \
        F.sum('covid_case_delta').alias('covid_case_delta'), \
        F.sum('covid_death_total').alias('covid_death_total'), \
        F.sum('covid_death_delta').alias('covid_death_delta'))
    
    state_facts_df.write.partitionBy('state').mode('append').parquet(output_path + "state_facts.parquet")
    

def load_state_facts_table(spark):
    '''
    Load state facts table from parquet

    Parameters:
    spark (SparkContext): Spark context to run operations on
    
    Returns:
    state_facts_df (Dataframe): Spark dataframe representing the state facts table
    '''
    
    state_facts_df = spark.read.option("basePath", output_path + "state_facts.parquet").parquet(output_path + "state_facts.parquet")
    
    return state_facts_df

def run_etl_pipeline(spark):
    '''
    Runs the ETL pipeline.
    - Read in the Covid-19 data sets
    - Read in the health data set
    - Create the time dimension table
    - Create the county dimension table
    - Create the state dimension table
    - Create the county facts table
    - Create the state facts table

    Parameters:
    spark (SparkContext): Spark context to run operations on
    
    Returns:
    time_df (Dataframe): Spark dataframe representing the time dimension table
    county_dim_df (Dataframe): Spark dataframe representing the county dimension table
    state_dim_df (Dataframe): Spark dataframe representing the state dimension table
    county_facts_df (Dataframe): Spark dataframe representing the county facts table
    state_facts_df (Dataframe): Spark dataframe representing the state facts table
    '''
    
    covid_cases_df = load_covid_case_data(spark)
    covid_deaths_df = load_covid_deaths_data(spark)
    
    health_df = load_health_data(spark)
    
    time_dim_df = create_time_dimension_table(spark)
    
    county_dim_df = create_county_dimension_table(spark, covid_cases_df, health_df)
    
    state_dim_df = create_state_dimension_table(spark, health_df, county_dim_dfs)
    
    county_facts_df = create_county_facts_table(spark)
    
    state_facts_df = create_state_facts_table(spark)
    
    return time_dim_df, county_dim_df, state_dim_df, county_facts_df, state_facts_df


def main():
    '''
    Creates the spark session, then runs the whole ETL pipeline
    '''
    
    spark = create_spark_session()
    
    run_etl_pipeline(spark)
    

if __name__ == "__main__":
    main()