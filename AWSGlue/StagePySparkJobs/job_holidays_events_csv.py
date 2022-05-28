from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DateType
from pyspark.sql.window import Window
import pyspark.sql.functions as f
import boto3
from awsglue.utils import getResolvedOptions
import sys


class tech_func:
    def __init__(self):
        pass

    @staticmethod
    def digits(orginal_value):
        return f.regexp_replace(orginal_value, r"[^0-9]", "")

    @staticmethod
    def capitalize(string_col):
        return f.initcap(string_col)


def main_load_tmp_holidays_events():
    spark = SparkSession.builder \
        .appName('DataFrame') \
        .master('local[*]') \
        .getOrCreate()

    data_path = 's3://tmgluesourcebucket//holidays_events.csv'

    args = getResolvedOptions(sys.argv, ['ACCOUNT', 'WAREHOUSE', 'DB', 'SCHEMA', 'USERNAME', 'PASSWORD'])

    sf_params_dwh_store_stage = {
        "sfURL": args['ACCOUNT'],
        "sfUser": args['USERNAME'],
        "sfPassword": args['PASSWORD'],
        "sfDatabase": args['DB'],
        "sfSchema": args['SCHEMA'],
        "sfWarehouse": args['WAREHOUSE']
    }

    schema_holiday_events = StructType([
        StructField("DATE", DateType())
        , StructField("TYPE", StringType())
        , StructField("LOCALE", StringType())
        , StructField("LOCALE_NAME", StringType())
        , StructField("DESCRIPTION", StringType())
        , StructField("TRANSFERRED", StringType())
    ])
    df_holidays_event = spark.read.option("inferSchema", "true") \
        .option("header", "true") \
        .csv(data_path, schema_holiday_events)

    df_ready_holidays_event = df_holidays_event.select("DATE", "TYPE", "LOCALE", "LOCALE_NAME", "DESCRIPTION",
                                                       "TRANSFERRED", \
                                                       f.row_number().over(Window.partitionBy(f.col("DATE")) \
                                                                           .orderBy(
                                                           f.when(f.col("LOCALE") == 'National', 1) \
                                                           .when(f.col("LOCALE") == "Regional", 2) \
                                                           .when(f.col("LOCALE") == "Local", 3) \
                                                           .otherwise(4))).alias("Rank")) \
        .filter(f.col("Rank") == 1) \
        .filter(f.col("TYPE") != 'Work Day') \
        .drop(f.col("Rank"))

    df_ready_holidays_event.write.format("snowflake") \
        .options(**sf_params_dwh_store_stage) \
        .option("dbtable", "TMP_HOLIDAYS_EVENTS") \
        .mode("append") \
        .save()

    glue_jobs = 'DIM_DATE_DATA_MART'
    glue = boto3.client('glue')
    response = glue.start_job_run(JobName=glue_jobs)


main_load_tmp_holidays_events()