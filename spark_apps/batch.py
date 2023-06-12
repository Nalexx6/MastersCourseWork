import logging
import argparse
import os
import utils
import metrics

logging.basicConfig(format='%(asctime)s: %(levelname)s: %(module)s: %(message)s', level=logging.INFO)

from pyspark.sql.types import StringType, StructType, StructField, DateType, IntegerType, DecimalType


def update_metrics(spark, config):

    logging.info('Reading datasets')
    df = utils.add_date_cols(spark.read.parquet(os.path.join(config['root'], config['output_table'])))
    licenses = spark.read.csv(os.path.join(config['root'], config['licences_table']), header=True)
    zones = spark.read.csv(os.path.join(config['root'], config['zones_table']), header=True)

    logging.info('Updating totals by pickup location')
    utils.save_metrics(metrics.total_by_pickup_loc(df, zones), 'trips_by_pickup_locations')

    logging.info('Updating totals by dropoff location')
    utils.save_metrics(metrics.total_by_dropoff_loc(df, zones), 'trips_by_dropoff_locations')

    logging.info('Updating top licenses by an hour')
    utils.save_metrics(metrics.top_licenses_by_hour(df, licenses), 'top_licenses_by_hour')

    logging.info('Updating average tip by trip distance')
    utils.save_metrics(metrics.avg_tip_by_trip_distance(df), 'avg_tip_by_trip_distance')

    logging.info('Updating average tip by trip duration')
    utils.save_metrics(metrics.avg_tip_by_trip_duration(df), 'avg_tip_by_trip_duration')

    logging.info('Updating average tip by trip speed')
    utils.save_metrics(metrics.avg_tip_by_trip_speed(df), 'avg_tip_by_trip_speed')

    logging.info('Data update finished')


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="spark_test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--local", type=bool, default=False)

    args = parser.parse_args()

    spark = utils.create_spark_session(app_name='batch', local=args.local)

    config = {
        'kafka_brokers': ['localhost:9092'],
        'topics': ['data-topic'],
        'root': '',
        'checkpoint_location': 'nalexx-bucket/data/checkpoint',
        'output_table': 'fhvhv_tripdata_2023-01.parquet',
        'licences_table': 'licenses.csv',
        'zones_table': 'zones.csv',
        'partition_key': 'hvfhs_license_num',
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

    update_metrics(spark, config)
