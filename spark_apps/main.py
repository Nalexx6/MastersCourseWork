import logging
import argparse
import os.path
from datetime import datetime

import utils

logging.basicConfig(format='%(asctime)s: %(levelname)s: %(module)s: %(message)s', level=logging.INFO)


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

    # logging.info(f'Dataset union took {datetime.now() - start}')
    perf_res['union'] = datetime.now() - start

    "============================================================================================================"

    start = datetime.now()

    print(f"The average trip duration is: {utils.calc_avg_trip_duraiton(union)} minutes")

    # logging.info(f'Calculation of mean trip duration took {datetime.now() - start}')
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

    return perf_res


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="medicare_age_65",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--scale", required=True)

    args = parser.parse_args()

    test_res = {}

    spark = utils.create_spark_session(app_name='test')

    files = ['fhvhv_tripdata_2023-01.parquet',
             'fhvhv_tripdata_2023-02.parquet',
             'fhvhv_tripdata_2022-01.parquet',
             'fhvhv_tripdata_2022-02.parquet',
             'fhvhv_tripdata_2022-03.parquet',
             'fhvhv_tripdata_2022-04.parquet',
             'fhvhv_tripdata_2022-05.parquet',
             'fhvhv_tripdata_2022-06.parquet',
             'fhvhv_tripdata_2022-07.parquet',
             'fhvhv_tripdata_2022-08.parquet',
             'fhvhv_tripdata_2022-09.parquet',
             'fhvhv_tripdata_2022-10.parquet',
             'fhvhv_tripdata_2022-11.parquet',
             'fhvhv_tripdata_2022-12.parquet']

    for s in range(2, int(args.scale) + 1):
        test_res[s] = test_scale(files[:s])

    for k, v in test_res.items():
        logging.info(f'============================== Results for scale {k} ======================================')
        for k1, v1 in v.items():
            logging.info(f'{k1} operation took {v1} seconds')