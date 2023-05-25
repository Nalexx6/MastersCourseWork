import pandas as pd
import logging
import os
from datetime import datetime
import functions as fn

logging.basicConfig(format='%(asctime)s: %(levelname)s: %(module)s: %(message)s', level=logging.INFO)

rename_config = {
    'VendorID': 'hvfhs_license_num',
    'tpep_pickup_datetime': 'pickup_datetime',
    'tpep_dropoff_datetime': 'dropoff_datetime',
    'total_amount': 'driver_pay',
    'tolls_amount': 'tolls',
    'tips_amount': 'tips'
}


def rename_columns(df):
    return df.rename(columns=rename_config)


def add_date_cols(df):

    df['pickup_day_of_week'] = df['pickup_datetime'].dt.dayofweek
    df['pickup_day'] = df['pickup_datetime'].dt.day
    df['pickup_hour'] = df['pickup_datetime'].dt.hour

    df['dropoff_day_of_week'] = df['dropoff_datetime'].dt.dayofweek
    df['dropoff_day'] = df['dropoff_datetime'].dt.day
    df['dropoff_hour'] = df['dropoff_datetime'].dt.hour

    return df


def read_df(path, ren=False):
    root = ''

    if ren:
        return add_date_cols(rename_columns(pd.read_parquet(os.path.join(root, path), engine='fastparquet')))
    else:
        return add_date_cols(pd.read_parquet(os.path.join(root, path), engine='fastparquet'))


if __name__ == "__main__":

    "============================================================================================================"

    start = datetime.now()

    jan_data = read_df('yellow_tripdata_2023-01.parquet', ren=True)
    feb_data = read_df('yellow_tripdata_2023-02.parquet', ren=True)
    
    # jan_data = read_df('fhvhv_tripdata_2023-01.parquet')
    # feb_data = read_df('fhvhv_tripdata_2023-02.parquet')
    # union = pd.read_parquet('fhvhv_tripdata_2023-01.parquet', engine='fastparquet')
    print(jan_data.columns)

    logging.info(f'Dataset reading took {datetime.now() - start}')

    "============================================================================================================"

    start = datetime.now()

    union = fn.union(jan_data, feb_data)

    logging.info(f'Dataset union took {datetime.now() - start}')
    "============================================================================================================"
    print(union.shape[0])

    start = datetime.now()

    print(f"The average trip duration is: {fn.calc_avg_trip_duraiton(union)} minutes")

    logging.info(f'Calculation of mean trip duration took {datetime.now() - start}')

    "============================================================================================================"

    start = datetime.now()

    fn.total_by_pickup_loc(union)

    logging.info(f'Calculation of total pickups by location took {datetime.now() - start}')

    "============================================================================================================"

    start = datetime.now()

    print(fn.avg_revenue_by_day_of_week(union).head(7))

    logging.info(f'Calculation of average revenue by day of week took {datetime.now() - start}')

    "============================================================================================================"

    start = datetime.now()

    print(fn.top_dropoff_zones_by_hour(union).head(25))

    logging.info(f'Calculation of top drop-off zones by hour took {datetime.now() - start}')

    "============================================================================================================"

    start = datetime.now()

    joined = fn.join_test(jan_data, feb_data)
    print(joined.shape[0])

    logging.info(f'Datasets join took {datetime.now() - start}')

    "============================================================================================================"

    # start = datetime.now()
    #
    # joined.to_parquet('./test_python.parquet', engine='fastparquet')
    #
    # logging.info(f'File writing took took {datetime.now() - start}')

    "============================================================================================================"

