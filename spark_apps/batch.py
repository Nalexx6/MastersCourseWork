import logging
import argparse
from datetime import datetime
import os
import utils

logging.basicConfig(format='%(asctime)s: %(levelname)s: %(module)s: %(message)s', level=logging.INFO)

from pyspark.sql.types import StringType, StructType, StructField, DateType, IntegerType, DecimalType
from pyspark.sql import DataFrame
import pyspark.sql.functions as fn


def test_scale(files):
    start = datetime.now()
    root = 's3a://nalexx-bucket/test'

    perf_res = {}

    dfs = [utils.read_df(spark, f, root) for f in files]

    logging.info(f'Dataset reading took {datetime.now() - start}')
    perf_res['reading'] = datetime.now() - start

    start = datetime.now()
    union = utils.union_all(dfs)
    print(union.count())

    logging.info(f'Dataset union took {datetime.now() - start}')
    perf_res['union'] = datetime.now() - start

    "============================================================================================================"

    start = datetime.now()

    print(f"The average trip duration is: {utils.calc_avg_trip_duraiton(union)} minutes")

    logging.info(f'Calculation of mean trip duration took {datetime.now() - start}')
    perf_res['average_trip_duration'] = datetime.now() - start

    "============================================================================================================"

    start = datetime.now()

    utils.total_by_pickup_loc(union).show()

    logging.info(f'Calculation of total pickups by location took {datetime.now() - start}')
    perf_res['total_by_pickup_loc'] = datetime.now() - start

    "============================================================================================================"

    start = datetime.now()

    utils.avg_revenue_by_day_of_week(union).show()

    logging.info(f'Calculation of average revenue by day of week took {datetime.now() - start}')
    perf_res['avg_revenue_by_day_of_week'] = datetime.now() - start

    "============================================================================================================"

    start = datetime.now()

    utils.top_dropoff_zones_by_hour(union).show()

    logging.info(f'Calculation of top drop-off zones by hour took {datetime.now() - start}')
    perf_res['top_dropoff_zones_by_hour'] = datetime.now() - start

    "============================================================================================================"

    start = datetime.now()

    utils.join_test(dfs[0], union).show()

    logging.info(f'Calculation of top drop-off zones by hour took {datetime.now() - start}')
    perf_res['join'] = datetime.now() - start

    return perf_res


def calculate_metrics(df: DataFrame):

    df = df.groupBy("PULocationID").agg(fn.count("*").alias("trip_count"))

    countries = spark.read.csv('taxi+_zone_lookup.csv', header=True)

    return df.withColumnRenamed('PULocationID', 'LocationID').join(countries, on='LocationID')



if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="spark_test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    # parser.add_argument("--scale", required=True)
    parser.add_argument("--local", type=bool, default=False)

    args = parser.parse_args()

    spark = utils.create_spark_session(app_name='batch', local=args.local)

    config = {
        'kafka_brokers': ['localhost:9092'],
        'topics': ['data-topic'],
        'root': '',
        'checkpoint_location': 'nalexx-bucket/data/checkpoint',
        'output_table': 'fhvhv_tripdata_2023-01.parquet',
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

    df = spark.read.parquet(os.path.join(config['root'], config['output_table']))

    utils.save_metrics(calculate_metrics(df), config)


    # for s in range(2, int(args.scale) + 1):
    #     test_res[s] = test_scale(files[:s])
    #
    # # for k, v in test_res.items():
    # #     logging.info(f'============================== Results for scale {k} ======================================')
    # #     for k1, v1 in v.items():
    # #         logging.info(f'{k1} operation took {v1} seconds')
    #
    # for k, v in test_res.items():
    #     logging.info(f'============================== Results for scale {k} ======================================')
    #     for k1, v1 in v.items():
    #         print(v1)