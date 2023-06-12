# pylint: disable=protected-access,line-too-long,unused-import
import argparse
import logging
import os
import utils

import pyspark.sql.functions as fn
from pyspark.sql.types import StringType, StructType, StructField, DateType, IntegerType, DecimalType
from pyspark.sql import DataFrame

logging.basicConfig(format='%(asctime)s: %(levelname)s: %(module)s: %(message)s', level=logging.INFO)


class Writer:
    def __init__(self, config, spark):
        self.spark = spark
        self.config = config

    def __call__(self, df: DataFrame, epoch_id):
        output_table = os.path.join(config['root'], config['output_table'])

        df = utils.add_date_cols((df.where(df.value.isNotNull())
                .select(fn.from_json(fn.col("value").cast("string"), self.config['schema']).alias("value"))
                .select('value.*')
                ))

        (df.write.partitionBy(config['partition_key'])
            .mode('append')
            .parquet(output_table))

        logging.info(f'Data was updated in {output_table}')


def run_streaming(config, args, spark):
    logging.info('staring streaming query')

    trigger_once = args.trigger_once

    if trigger_once:
        args = dict(once=True)
    else:
        args = dict(processingTime='60 seconds')

    writer = Writer(
        spark=spark,
        config=config
    )

    stream = (
        spark
        .readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', ','.join(config['kafka_brokers']))
        .option('subscribePattern', '|'.join(config['topics']))
        .option('startingOffsets', 'earliest')
        .load()


        .writeStream
        .foreachBatch(writer)
        .option('checkpointLocation', config['checkpoint_location'])
        .trigger(**args)
        .start()
        .awaitTermination()
    )

    if trigger_once:
        stream.awaitTermination()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='new trips streaming processor',
    )

    parser.add_argument('--trigger_once', action='store_true')
    args = parser.parse_args()

    # config_path = os.path.join(os.path.dirname(__file__), args.config)
    # config = apps.load_confg(
    #     config_path, db_name=args.db_name, db_type=args.db_type,
    # )

    config = {
        'kafka_brokers': ['52.212.78.62:9092'],
        'topics': ['data_topic'],
        'root': 'nalexx-bucket/data/storage',
        'checkpoint_location': 'nalexx-bucket/data/checkpoint',
        'output_table': 'nyc-trip-data.parquet',
        'licences_table': 'licenses.csv',
        'zones_table': 'zones.csv',
        'partition_key': 'Hvfhs_license_num',
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

    spark_job_name = "streaming"

    spark = utils.create_spark_session(
        spark_job_name
    )

    run_streaming(config, args, spark)
