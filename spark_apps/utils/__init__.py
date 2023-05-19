import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))


from pyspark.sql import SparkSession, DataFrame


def create_spark_session(app_name):

    defaults = {
        # 'spark.hadoop.fs.s3a.endpoint': config['s3']['endpoint'],
        # 'spark.sql.warehouse.dir': config['warehouse_location'],
        'master': "local[*]",
        'spark.sql.session.timeZone': 'UTC',
        'spark.driver.extraLibraryPath': '/opt/hadoop/lib/native',
        'spark.executor.extraLibraryPath': '/opt/hadoop/lib/native',
        'spark.driver.extraJavaOptions': '-XX:+UseG1GC',
        'spark.executor.extraJavaOptions': '-XX:+UseG1GC',
        'spark.sql.autoBroadcastJoinThreshold': -1,
        'spark.driver.memory': '1g'

    }

    builder = (SparkSession
        .builder
        .appName(app_name)
    )

    for key, value in {**defaults}.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()


# def read_parquet_file():

