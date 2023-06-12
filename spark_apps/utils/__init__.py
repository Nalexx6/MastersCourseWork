import sys
import os
from pyspark.sql import functions as fn

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, StructType, StructField, DateType, IntegerType, DecimalType


def create_spark_session(app_name, local=False):

    packages = [
        'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1',
        'org.apache.kafka:kafka-clients:2.8.0',
        'org.postgresql:postgresql:42.5.1',
        'org.apache.hadoop:hadoop-aws:3.2.1',
        'software.amazon.awssdk:bundle:2.15.40',
        'software.amazon.awssdk:url-connection-client:2.15.40',
    ]

    defaults = {
        'spark.hadoop.fs.s3a.endpoint': 'http://s3.eu-west-1.amazonaws.com',
        'spark.sql.session.timeZone': 'UTC',
        'spark.default.parallelism': 20,
        "spark.jars.packages": ",".join(packages),
    }

    builder = (SparkSession
        .builder

        .appName(app_name)
    )

    if local:
        builder = builder.master('local')

    for key, value in {**defaults}.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()


def parse_df_schema(config):
    pass


def load_config(config_path, db_url=None, db_pass=None):

    config = {
        'kafka_brokers': ['localhost:9092'],
        'topics': ['data-topic'],
        'root': 'nalexx-bucket/data/storage',
        'checkpoint_location': 'nalexx-bucket/data/checkpoint',
        'licences_table': 'licenses.csv',
        'zones_table': 'zones.csv',
        'partition_key': 'hvfhs_license_num',

        'db_pass': db_pass,
        'db_url': db_url,

        'schema': StructType([StructField('hvfhs_license_num', StringType()),
                 StructField('dispatching_base_num', StringType()),
                 StructField('originating_base_num', StringType()),
                 StructField('request_datetime', DateType()),
                 StructField('on_scene_datetime', DateType()),
                 StructField('pickup_datetime', DateType()),
                 StructField('dropoff_datetime', DateType()),
                 StructField('PULocationID', IntegerType()),
                 StructField('DOLocationID', IntegerType()),
                 StructField('trip_miles', DecimalType()),
                 StructField('trip_time', IntegerType()),
                 StructField('base_passenger_fare', DecimalType()),
                 StructField('tolls', DecimalType()),
                 StructField('bcf', DecimalType()),
                 StructField('sales_tax', DecimalType()),
                 StructField('congestion_surcharge', DecimalType()),
                 StructField('airport_fee', DecimalType()),
                 StructField('tips', DecimalType()),
                 StructField('driver_pay', DecimalType()),
                 StructField('shared_request_flag', StringType()),
                 StructField('shared_match_flag', StringType()),
                 StructField('access_a_ride_flag', StringType()),
                 StructField('wav_request_flag', StringType()),
                 StructField('wav_match_flag', StringType())])
    }

    return config


def save_metrics(df, config, table_name):

    (df.write.format("jdbc").option("url", f"jdbc:postgresql://{config['db_url']}:5432/test")
        .option("driver", "org.postgresql.Driver")
        .option("user", "postgres")
        .option("password", config['db_pass'])
        .option("dbtable", f"public.{table_name}")
        .option("truncate", "true").mode("overwrite")
        .save())


def union_all(dfs):
    if len(dfs) > 1:
        return dfs[0].unionByName(union_all(dfs[1:]), allowMissingColumns=True)
    else:
        return dfs[0]


def rename_cols(df: DataFrame):
    return (df.withColumnRenamed('VendorID', 'hvfhs_license_num')
            .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime')
            .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')
            .withColumnRenamed('total_amount', 'driver_pay')
            .withColumnRenamed('tolls_amount', 'tolls')
            .withColumnRenamed('tips_amount', 'tips'))


def add_date_cols(df: DataFrame):

    return (df.withColumn('pickup_day_of_week', fn.dayofweek(df['pickup_datetime']))
            .withColumn('pickup_day', fn.dayofmonth(df['pickup_datetime']))
            .withColumn('pickup_hour', fn.hour(df['pickup_datetime']))
            .withColumn('dropoff_day_of_week', fn.dayofweek(df['dropoff_datetime']))
            .withColumn('dropoff_day', fn.dayofmonth(df['dropoff_datetime']))
            .withColumn('dropoff_hour', fn.hour(df['dropoff_datetime'])))


def read_df(spark, path, root, ren=False):

    if ren:
        return rename_cols(spark.read.parquet(os.path.join(root, path)))
    else:
        return spark.read.parquet(os.path.join(root, path))