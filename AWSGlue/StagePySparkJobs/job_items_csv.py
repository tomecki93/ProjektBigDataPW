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


def main_load_tmp_items():
    spark = SparkSession.builder \
        .appName('DataFrame') \
        .master('local[*]') \
        .getOrCreate()

    # data_path = ''
    data_path = 's3://tmgluesourcebucket//items.csv'

    args = getResolvedOptions(sys.argv, ['ACCOUNT', 'WAREHOUSE', 'DB', 'SCHEMA', 'USERNAME', 'PASSWORD'])

    sf_params_dwh_store_stage = {
        "sfURL": args['ACCOUNT'],
        "sfUser": args['USERNAME'],
        "sfPassword": args['PASSWORD'],
        "sfDatabase": args['DB'],
        "sfSchema": args['SCHEMA'],
        "sfWarehouse": args['WAREHOUSE']
    }

    schema_items = StructType([
        StructField("ITEM_NBR", StringType())
        , StructField("FAMILY", StringType())
        , StructField("CLASS", StringType())
        , StructField("PERISHABLE", StringType())
    ])
    df_items = spark.read.option("inferSchema", "true") \
        .option("header", "true") \
        .option("mode", "DROPMALFORMED") \
        .csv(data_path, schema_items)

    df_items = df_items.select("ITEM_NBR", "FAMILY" \
                               , tech_func.digits(f.col("CLASS")).alias("CLASS") \
                               , tech_func.digits(f.col("PERISHABLE")).alias("PERISHABLE")) \
        .filter(f.col("ITEM_NBR") > 0)

    df_items.write.format("snowflake") \
        .options(**sf_params_dwh_store_stage) \
        .option("dbtable", "TMP_ITEMS") \
        .mode("append") \
        .save()

    glue_jobs = 'DIM_ITEMS_DATA_MART'
    glue = boto3.client('glue')
    response = glue.start_job_run(JobName=glue_jobs)


main_load_tmp_items()


