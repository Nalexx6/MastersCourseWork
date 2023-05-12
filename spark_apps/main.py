import pandas as pd
import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame

logging.basicConfig(format='%(asctime)s: %(levelname)s: %(module)s: %(message)s', level=logging.INFO)


def create_spark_session(app_name, config_override={}):
    default_config = {}

    # defaults = {
    #     'spark.hadoop.fs.s3a.endpoint': config['s3']['endpoint'],
    #     'spark.sql.warehouse.dir': config['warehouse_location'],
    #     'spark.sql.session.timeZone': 'UTC',
    # }

    # default_config.update(defaults)

    builder = (SparkSession
        .builder
        .appName(app_name)
    )

    # for key, value in {**default_config, **config_override}.items():
    #     builder = builder.config(key, value)

    return builder.getOrCreate()


if __name__ == "__main__":

    spark = create_spark_session(app_name='test')

    start = datetime.now()

    df = spark.read.parquet('fhvhv_tripdata_2023-01.parquet')

    logging.info(f'Dataset reading took {datetime.now() - start}')

    start = datetime.now()
    df.count()
    logging.info(f'Dataset counting took {datetime.now() - start}')
