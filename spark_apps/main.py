import logging
import argparse
from datetime import datetime

import utils

logging.basicConfig(format='%(asctime)s: %(levelname)s: %(module)s: %(message)s', level=logging.INFO)


# def main(spark, config):

if __name__ == "__main__":

    spark = utils.create_spark_session(app_name='test')

    start = datetime.now()

    df = spark.read.parquet('fhvhv_tripdata_2023-01.parquet')
    df1 = spark.read.parquet('fhvhv_tripdata_2023-02.parquet')

    logging.info(f'Dataset reading took {datetime.now() - start}')

    start = datetime.now()
    # df = df.union(df1)
    logging.info(f'Dataset union took {datetime.now() - start}')

    start = datetime.now()
    # print(df.count())
    logging.info(f'Dataset counting took {datetime.now() - start}')

    start = datetime.now()
    print(df.distinct().count())
    logging.info(f'Dataset distinct counting took {datetime.now() - start}')

    # df.show(10)
