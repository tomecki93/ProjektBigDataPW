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


def main_load_tmp_events():
    spark = SparkSession.builder \
        .appName('DataFrame') \
        .master('local[*]') \
        .getOrCreate()

    data_path = 's3://tmgluesourcebucket//events.csv'
    
	args = getResolvedOptions(sys.argv, ['ACCOUNT', 'WAREHOUSE', 'DB', 'SCHEMA', 'USERNAME', 'PASSWORD'])

    sf_params_dwh_store_stage = {
        "sfURL": args['ACCOUNT'],
        "sfUser": args['USERNAME'],
        "sfPassword": args['PASSWORD'],
        "sfDatabase": args['DB'],
        "sfSchema": args['SCHEMA'],
        "sfWarehouse": args['WAREHOUSE']
    }

    schema_events = StructType([
        StructField("ID", IntegerType())
        , StructField("DATE", DateType())
        , StructField("STORE_NBR", IntegerType())
        , StructField("ITEM_NBR", StringType())
        , StructField("UNIT_SALES", StringType())
        , StructField("ONPROMOTION", StringType())
    ])
    df_events = spark.read.option("inferSchema", "true") \
        .option("header", "true") \
        .csv(data_path, schema_events)

    df_events.write.format("snowflake") \
        .options(**sf_params_dwh_store_stage) \
        .option("dbtable", "TMP_ALL_EVENTS") \
        .mode("append") \
        .save()

    glue_jobs = 'FACT_EVENTS_DATA_MART'
    glue = boto3.client('glue')
    response = glue.start_job_run(JobName=glue_jobs)


main_load_tmp_events()


