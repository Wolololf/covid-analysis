import configparser

from pyspark.sql import SparkSession

def create_spark_session():
    """
    Creates the spark session
    
    Returns:
    Newly created spark session
    """

    config = configparser.ConfigParser()
    config.read('config.cfg')

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()

    return spark