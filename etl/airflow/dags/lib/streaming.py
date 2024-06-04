# pylint: disable=protected-access,line-too-long,unused-import
import argparse
import logging
import os
import utils

import pyspark.sql.functions as fn
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

    parser.add_argument("--config", type=str, required=True)
    parser.add_argument('--trigger_once', action='store_true')

    args = parser.parse_args()
    config_path = os.path.join(os.path.dirname(__file__), args.config)

    spark = utils.create_spark_session(app_name='streaming')
    config = utils.load_config(config_path)

    run_streaming(config, args, spark)
