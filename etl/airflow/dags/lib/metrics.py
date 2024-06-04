import sys
import os
from pyspark.sql import functions as fn

sys.path.append(os.path.join(os.path.dirname(__file__), ''))

from pyspark.sql import DataFrame, Window


def total_by_pickup_loc(df: DataFrame, zones):
    df = df.groupBy("Hvfhs_license_num", "PULocationID").agg((fn.count("*") + 1).alias("trip_count"))

    return df.withColumnRenamed('PULocationID', 'LocationID').join(zones, on='LocationID')


def total_by_dropoff_loc(df: DataFrame, zones):

    df = df.groupBy("Hvfhs_license_num", "DOLocationID").agg((fn.count("*") + 1).alias("trip_count"))

    return df.withColumnRenamed('DOLocationID', 'LocationID').join(zones, on='LocationID')


def top_licenses_by_hour(df:DataFrame, licenses):
    trips_by_hour_license = df.groupBy("Hvfhs_license_num", "dropoff_hour").count()

    window_spec = Window.partitionBy("dropoff_hour").orderBy(fn.desc("count"))
    df = trips_by_hour_license.withColumn("rank", fn.row_number().over(window_spec))

    return df.join(licenses, on='Hvfhs_license_num')


def avg_tip_by_trip_distance(df):
    return df.groupby('trip_miles').agg(fn.avg("tips").alias("avg_tips"))


def avg_tip_by_trip_duration(df: DataFrame):
    return df.groupby('trip_time').agg(fn.avg("tips").alias("avg_tips"))\
        .withColumn('trip_time', fn.col('trip_time') / 60)


def avg_tip_by_trip_speed(df: DataFrame):
    df = df.withColumn('trip_speed', fn.col('trip_miles') / fn.col('trip_time') * 3600)

    return df.groupby('trip_speed').agg(fn.avg("tips").alias("avg_tips"))
