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


def main_load_tmp_transactions():
    spark = SparkSession.builder \
        .appName('DataFrame') \
        .master('local[*]') \
        .getOrCreate()

    data_path = 's3://tmgluesourcebucket//transactions.csv'

    args = getResolvedOptions(sys.argv, ['ACCOUNT', 'WAREHOUSE', 'DB', 'SCHEMA', 'USERNAME', 'PASSWORD'])

    sf_params_dwh_store_stage = {
        "sfURL": args['ACCOUNT'],
        "sfUser": args['USERNAME'],
        "sfPassword": args['PASSWORD'],
        "sfDatabase": args['DB'],
        "sfSchema": args['SCHEMA'],
        "sfWarehouse": args['WAREHOUSE']
    }

    schema_transactions = StructType([
        StructField("DATE", DateType())
        , StructField("STORE_NBR", IntegerType())
        , StructField("TRANSACTIONS", StringType())
    ])
    df_transactions = spark.read.option("inferSchema", "true") \
        .option("header", "true") \
        .csv(data_path, schema_transactions)

    df_transactions = df_transactions.select("DATE", "STORE_NBR", \
                                             tech_func.digits(f.col("TRANSACTIONS")).alias("TRANSACTIONS"))

    df_transactions.write.format("snowflake") \
        .options(**sf_params_dwh_store_stage) \
        .option("dbtable", "TMP_TRANSACTIONS") \
        .mode("append") \
        .save()

    glue_jobs = 'FACT_TRANSACTIONS_DATA_MART'
    glue = boto3.client('glue')
    response = glue.start_job_run(JobName=glue_jobs)


main_load_tmp_transactions()


