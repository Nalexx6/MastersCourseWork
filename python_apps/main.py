import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(format='%(asctime)s: %(levelname)s: %(module)s: %(message)s', level=logging.INFO)


if __name__ == "__main__":

    start = datetime.now()

    df = pd.read_parquet('../fhvhv_tripdata_2023-01.parquet', engine='fastparquet')

    logging.info(f'Dataset writing took {datetime.now() - start}')

    # print(df.head(5))

    start = datetime.now()
    df.count()
    logging.info(f'Dataset counting took {datetime.now() - start}')
