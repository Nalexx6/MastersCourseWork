import sys
import os
from pyspark.sql import functions as fn

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))


from pyspark.sql import SparkSession, DataFrame, Window


def create_spark_session(app_name):

    # os.environ['JAVA_HOME'] = '$(/usr/libexec/java_home -v 1.8)'

    defaults = {
        # 'spark.hadoop.fs.s3a.endpoint': config['s3']['endpoint'],
        # 'spark.sql.warehouse.dir': config['warehouse_location'],
        # 'master': "local[*]",
        'spark.sql.session.timeZone': 'UTC',
        'spark.driver.extraLibraryPath': '/opt/hadoop/lib/native',
        'spark.executor.extraLibraryPath': '/opt/hadoop/lib/native',
        'spark.driver.extraJavaOptions': '-XX:+UseG1GC',
        'spark.executor.extraJavaOptions': '-XX:+UseG1GC',
        # 'spark.sql.autoBroadcastJoinThreshold': -1,
        'spark.executor.cores': '2',
        'spark.executor.memory': '5g',
        'spark.driver.memory': '5g',
        'spark.driver.extraClassPath': '/home/moleksiienko/pyspark/*'

    }

    builder = (SparkSession
        .builder
        .appName(app_name)
        .master('local')
    )

    for key, value in {**defaults}.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()


def rename_cols(df: DataFrame):
    return (df.withColumnRenamed('VendorID', 'hvfhs_license_num')
            .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime')
            .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')
            .withColumnRenamed('total_amount', 'driver_pay')
            .withColumnRenamed('tolls_amount', 'tolls')
            .withColumnRenamed('tips_amount', 'tips'))


def add_date_cols(df: DataFrame):

    return (df.withColumn('pickup_day_of_week', fn.dayofweek(df['pickup_datetime']))
            .withColumn('pickup_day', fn.dayofmonth(df['pickup_datetime']))
            .withColumn('pickup_hour', fn.hour(df['pickup_datetime']))
            .withColumn('dropoff_day_of_week', fn.dayofweek(df['dropoff_datetime']))
            .withColumn('dropoff_day', fn.dayofmonth(df['dropoff_datetime']))
            .withColumn('dropoff_hour', fn.hour(df['dropoff_datetime'])))


def read_df(spark, path, root, ren=False):

    if ren:
        return add_date_cols(rename_cols(spark.read.parquet(os.path.join(root, path))))
    else:
        return add_date_cols(spark.read.parquet(os.path.join(root, path)))


def union(df1, df2):
    return df1.union(df2)


def calc_avg_trip_duraiton(df: DataFrame):
    df = df.withColumn("trip_duration",
                       (fn.unix_timestamp(df.dropoff_datetime) -  fn.unix_timestamp(df.pickup_datetime)) / 60)

    # Calculate the average trip duration
    return df.select(fn.avg("trip_duration")).first()[0]


def total_by_pickup_loc(df):
    return df.groupBy("PULocationID").agg(fn.count("*").alias("trip_count"))


def avg_revenue_by_day_of_week(df):
    # Calculate the average revenue by day of the week
    return df.groupby('pickup_day_of_week').agg(fn.avg("driver_pay").alias("average_revenue"))


def top_dropoff_zones_by_hour(df):

    # Group df by hour and dropoff location, and count the number of trips
    trips_by_hour_location = df.groupBy("dropoff_hour", "DOLocationID").count()

    # Rank the dropoff zones within each hour based on the number of trips
    window_spec = Window.partitionBy("dropoff_hour").orderBy(fn.desc("count"))
    ranked_trips = trips_by_hour_location.withColumn("rank", fn.row_number().over(window_spec))

    # Filter for the top dropoff zone by hour (rank = 1)
    return ranked_trips.filter(ranked_trips.rank == 1)


def join_test(jan, feb):
    return (jan.join(feb, on=['hvfhs_license_num', 'pickup_day', 'pickup_hour', 'pickup_day_of_week'], how='outer')
            .selectExpr('hvfhs_license_num', 'pickup_day', 'pickup_hour', 'pickup_day_of_week'))