import logging
import argparse
import os
import utils
import metrics

logging.basicConfig(format='%(asctime)s: %(levelname)s: %(module)s: %(message)s', level=logging.INFO)


def update_metrics(spark, config):

    logging.info('Reading datasets')
    df = utils.add_date_cols(spark.read.parquet(os.path.join(config['root'], config['output_table'])))
    licenses = spark.read.csv(os.path.join(config['root'], config['licences_table']), header=True)
    zones = spark.read.csv(os.path.join(config['root'], config['zones_table']), header=True)

    logging.info('Updating totals by pickup location')
    utils.save_metrics(metrics.total_by_pickup_loc(df, zones), config, 'trips_by_pickup_locations')

    logging.info('Updating totals by dropoff location')
    utils.save_metrics(metrics.total_by_dropoff_loc(df, zones), config, 'trips_by_dropoff_locations')

    logging.info('Updating top licenses by an hour')
    utils.save_metrics(metrics.top_licenses_by_hour(df, licenses), config, 'top_licenses_by_hour')

    logging.info('Updating average tip by trip distance')
    utils.save_metrics(metrics.avg_tip_by_trip_distance(df), config, 'avg_tip_by_trip_distance')

    logging.info('Updating average tip by trip duration')
    utils.save_metrics(metrics.avg_tip_by_trip_duration(df), config, 'avg_tip_by_trip_duration')

    logging.info('Updating average tip by trip speed')
    utils.save_metrics(metrics.avg_tip_by_trip_speed(df), config, 'avg_tip_by_trip_speed')

    logging.info('Data update finished')


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="spark_test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("--config", type=str, required=True)
    parser.add_argument('--db-url', type=str, required=True)
    parser.add_argument('--db-pass', type=str, required=True)

    args = parser.parse_args()
    config_path = os.path.join(os.path.dirname(__file__), args.config)

    spark = utils.create_spark_session(app_name='batch')
    config = utils.load_config(config_path, args.db_url, args.db_pass)

    update_metrics(spark, config)
