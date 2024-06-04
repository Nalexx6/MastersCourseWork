import sys
import os
import yaml

from yaml import Loader
from pyspark.sql import functions as fn

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, StructType, StructField, DateType, IntegerType, DecimalType


def create_spark_session(app_name, local=False):

    packages = [
        'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1',
        'org.apache.kafka:kafka-clients:2.8.0',
        'org.postgresql:postgresql:42.5.1',
        'org.apache.hadoop:hadoop-aws:3.3.4'
    ]

    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    print(aws_secret_access_key)

    defaults = {
        'spark.hadoop.fs.s3a.endpoint': 'http://s3.eu-west-1.amazonaws.com',
        'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
        'spark.sql.session.timeZone': 'UTC',
        'spark.default.parallelism': 20,
        "spark.jars.packages": ",".join(packages),
        "spark.hadoop.fs.s3a.access.key": aws_access_key_id,
        "spark.hadoop.fs.s3a.secret.key": aws_secret_access_key

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
    type_mapping = {
        'str': StringType(),
        'date': DateType(),
        'int': IntegerType(),
        'decimal': DecimalType(11, 2),

    }

    fields = []

    for f in config['schema']:
        fields.append(StructField(f['field_name'], type_mapping[f['type']].__class__()))

    return StructType(fields)


def load_yaml(path):
    with open(path) as f:
        data = yaml.load(f, Loader=Loader)
    return data


def load_config(path, db_url=None, db_pass=None):

    config_params = load_yaml(path)

    print(config_params)

    db_params = {
        'db_pass': db_pass,
        'db_url': db_url,
    }

    config = {
        **config_params,
        **db_params,
        'schema': parse_df_schema(config_params)
    }

    return config


def save_metrics(df, config, table_name):

    (df.write.format("jdbc").option("url", f"jdbc:postgresql://{config['db_url']}:5432/postgres")
        .option("driver", "org.postgresql.Driver")
        .option("user", "postgres")
        .option("password", config['db_pass'])
        .option("dbtable", f"public.{table_name}")
        .option("truncate", "true").mode("append")
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