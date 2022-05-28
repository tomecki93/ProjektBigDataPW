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


def main_load_tmp_stores():
    spark = SparkSession.builder \
        .appName('DataFrame') \
        .master('local[*]') \
        .getOrCreate()

    data_path = 's3://tmgluesourcebucket//stores.json'

    args = getResolvedOptions(sys.argv, ['ACCOUNT', 'WAREHOUSE', 'DB', 'SCHEMA', 'USERNAME', 'PASSWORD'])

    sf_params_dwh_store_stage = {
        "sfURL" : args['ACCOUNT'],
        "sfUser" : args['USERNAME'],
        "sfPassword" : args['PASSWORD'],
        "sfDatabase" : args['DB'],
        "sfSchema" : args['SCHEMA'],
        "sfWarehouse" : args['WAREHOUSE']
    }

    schema_stores = StructType([
        StructField("store_nbr", StringType())
        , StructField("city", StringType())
        , StructField("state", StringType())
        , StructField("type", StringType())
        , StructField("cluster", StringType())
    ])
    df_stores = spark.read \
        .option("multiline", "true") \
        .schema(schema_stores) \
        .json(data_path)

    df_stores = df_stores.select((tech_func.digits(f.col('store_nbr')).cast(IntegerType())).alias("STORE_NBR") \
                                 , (tech_func.capitalize(f.col("CITY")).alias("CITY")) \
                                 , (tech_func.capitalize(f.col("STATE")).alias("STATE")) \
                                 , (tech_func.capitalize(f.col("TYPE")).alias("TYPE")) \
                                 , (tech_func.digits(f.col("CLUSTER")).alias("CLUSTER")))

    df_stores.write.format("snowflake") \
        .options(**sf_params_dwh_store_stage) \
        .option("dbtable", "TMP_STORES") \
        .mode("append") \
        .save()

    glue_jobs = 'DIM_STORE_DATA_MART'
    glue = boto3.client('glue')
    response = glue.start_job_run(JobName=glue_jobs)


main_load_tmp_stores()