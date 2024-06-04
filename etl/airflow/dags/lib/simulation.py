import time

import utils
import json
from bson import json_util
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from datetime import datetime
import logging
import argparse
import os
import random

logging.basicConfig(format='%(asctime)s: %(levelname)s: %(module)s: %(message)s', level=logging.INFO)


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='new trips streaming processor',
    )
    parser.add_argument("--config", type=str, required=True)

    args = parser.parse_args()
    config_path = os.path.join(os.path.dirname(__file__), args.config)

    spark = utils.create_spark_session(app_name='simulation', local=True)
    config = utils.load_config(config_path)

    # files = ['fhvhv_tripdata_2023-01.parquet']
    # admin_client = KafkaAdminClient(
    #     bootstrap_servers=config['kafka_brokers'],
    #     client_id='simulation'
    # )

    # topic_list = []
    # topic_list.append(NewTopic(name="nyc-taxi-topic", num_partitions=1, replication_factor=1))
    # admin_client.create_topics(new_topics=topic_list, validate_only=False)

    df = utils.read_df(spark, './fhvhv_tripdata_2024-03.parquet', '').limit(20)
    # df.show(20)
    df = df.collect()

    for batch in chunker(df, 10):

        logging.info(json.dumps('Preparing new trips to be sent'))

        producer = KafkaProducer(bootstrap_servers=config['kafka_brokers'])

        for t in batch:
            producer.send('data_topic', json.dumps(t, default=json_util.default).encode('utf-8'))
            producer.flush()

        logging.info(json.dumps('New trips have been sent'))
        time.sleep(random.randint(30, 60))
