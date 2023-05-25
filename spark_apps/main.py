import logging
import argparse
from datetime import datetime

import utils

logging.basicConfig(format='%(asctime)s: %(levelname)s: %(module)s: %(message)s', level=logging.INFO)


# def main(spark, config):


if __name__ == "__main__":

    spark = utils.create_spark_session(app_name='test')

    start = datetime.now()

    jan_data = utils.read_df(spark, 'yellow_tripdata_2023-01.parquet', '', ren=True)
    feb_data = utils.read_df(spark, 'yellow_tripdata_2023-02.parquet', '', ren=True)

    # jan_data = utils.read_df(spark, 'fhvhv_tripdata_2023-01.parquet', '')
    # feb_data = utils.read_df(spark, 'fhvhv_tripdata_2023-02.parquet', '')

    logging.info(f'Dataset reading took {datetime.now() - start}')

    start = datetime.now()
    union = utils.union(jan_data, feb_data)
    print(union.count())

    logging.info(f'Dataset union took {datetime.now() - start}')

    "============================================================================================================"

    start = datetime.now()

    print(f"The average trip duration is: {utils.calc_avg_trip_duraiton(union)} minutes")

    logging.info(f'Calculation of mean trip duration took {datetime.now() - start}')

    "============================================================================================================"

    start = datetime.now()

    utils.total_by_pickup_loc(union).show()

    logging.info(f'Calculation of total pickups by location took {datetime.now() - start}')

    "============================================================================================================"

    start = datetime.now()

    utils.avg_revenue_by_day_of_week(union).show()

    logging.info(f'Calculation of average revenue by day of week took {datetime.now() - start}')

    "============================================================================================================"

    start = datetime.now()

    utils.top_dropoff_zones_by_hour(union).show()

    logging.info(f'Calculation of top drop-off zones by hour took {datetime.now() - start}')

    "============================================================================================================"

    start = datetime.now()

    joined = utils.join_test(jan_data, feb_data)
    print(joined.count())

    logging.info(f'Datasets join took {datetime.now() - start}')

    "============================================================================================================"

    # start = datetime.now()
    #
    # joined.to_parquet('./test_python.parquet', engine='fastparquet')
    #
    # logging.info(f'File writing took took {datetime.now() - start}')

    "============================================================================================================"
