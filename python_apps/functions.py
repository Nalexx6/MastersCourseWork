import pandas as pd


def union(df1, df2):
    df1 = df1.reset_index(drop=True)
    df2 = df2.reset_index(drop=True)
    return pd.concat([df1, df2])


def calc_avg_trip_duraiton(df):

    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])

    # Calculate trip duration in minutes
    df['trip_duration'] = (df['dropoff_datetime'] - df['pickup_datetime']).dt.total_seconds() / 60

    # Calculate the average trip duration
    return df['trip_duration'].mean()


def total_by_pickup_loc(df):
    return df['PULocationID'].value_counts()


def avg_revenue_by_day_of_week(df):
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])

    # Calculate the revenue for each trip
    df['revenue'] = df['driver_pay']

    # Extract the day of the week from the pickup datetime
    df['pickup_day_of_week'] = df['pickup_datetime'].dt.dayofweek

    # Calculate the average revenue by day of the week
    return df.groupby('pickup_day_of_week')['revenue'].mean()


def top_dropoff_zones_by_hour(df):

    # Convert dropoff datetime column to datetime format
    df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])

    # Extract the hour from the dropoff datetime
    df['dropoff_hour'] = df['dropoff_datetime'].dt.hour

    # Group df by hour and dropoff location, and count the number of trips
    trips_by_hour_location = df.groupby(['dropoff_hour', 'DOLocationID']).size().reset_index(name='trip_count')

    # Sort the df by trip count in descending order
    sorted_trips = trips_by_hour_location.sort_values(by='trip_count', ascending=False)

    # Get the top dropoff zones by hour
    return sorted_trips.groupby('dropoff_hour').head(1)


def join_test(jan: pd.DataFrame, feb: pd.DataFrame):
    jan.set_index(['hvfhs_license_num', 'pickup_day', 'pickup_hour', 'pickup_day_of_week'], inplace=True)
    feb.set_index(['hvfhs_license_num', 'pickup_day', 'pickup_hour', 'pickup_day_of_week'], inplace=True)

    return jan.join(feb, lsuffix='jan', rsuffix='feb')