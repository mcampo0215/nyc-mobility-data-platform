from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = PROJECT_ROOT / "data/raw/yellow_tripdata_2024-01.parquet"

spark = (
    SparkSession.builder
    .appName("NYC Mobility Data Cleaning")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.parquet(str(DATA_PATH))

print("RAW RECORD COUNT:")
print(df.count())

df = df.withColumn(
    'trip_duration_min',
    (
        F.unix_timestamp('tpep_dropoff_datetime') - F.unix_timestamp('tpep_pickup_datetime')
    ) / 60
)

clean_df = df.filter(
    F.col('trip_duration_min') >= 0
)

rejected_df = df.filter(
    F.col('trip_duration_min') < 0
)

negative_fares_df = clean_df.filter(
    F.col('fare_amount') < 0
)

clean_df = clean_df.withColumn(
    'is_negative_fare',
    F.col('fare_amount') < 0
)

clean_df = clean_df.withColumn(
    'is_passenger_count_missing',
    F.col('passenger_count').isNull()
)

long_distance_df = clean_df.filter(
    F.col('trip_distance') > 100
)

clean_df = clean_df.withColumn(
    'avg_speed_mph',
    F.when(
        F.col('trip_duration_min') > 0,
        F.col('trip_distance')/(F.col('trip_duration_min')/60)
    ).otherwise(None)
)

clean_df = clean_df.withColumn(
    'is_speed_outlier',
    F.col('avg_speed_mph') > 100
)

long_duration_df = clean_df.filter(
    F.col('trip_duration_min') > 180
)

print('CLEANED RECORD COUNT:')
print(clean_df.count())

print ('REJECTED RECORD COUNT:')
print(rejected_df.count())

print('REJECTED RECORDS:')
rejected_df.select(
    'tpep_pickup_datetime',
    'tpep_dropoff_datetime',
    'trip_duration_min',
    'trip_distance'
).show(10, truncate=False)

print('NEGATIVE FARES RECORD COUNT:')
print(negative_fares_df.count())

print('NEGATIVE FARES RECORDS:')
negative_fares_df.select(
    'payment_type',
    'fare_amount',
    'total_amount',
    'trip_distance',
    'trip_duration_min'
).show(10, truncate=False)

print('NEGATIVE FARES BY PAYMENT TYPE:')
negative_fares_df.groupBy(
    'payment_type'
).count().orderBy(
    F.col('count').desc()
).show()

print('NEGATIVE FARE FLAGS:')
clean_df.select(
    'fare_amount',
    'payment_type',
    'is_negative_fare'
).show(10, truncate=False)

print('MISSING PASSENGER COUNT FLAGS:')

clean_df.filter(
    F.col('is_passenger_count_missing') == True
).select(
    'passenger_count',
    'trip_distance',
    'is_passenger_count_missing'
).show(10, truncate=False)

print('TRIPS OVER 100 MILES:')
print(long_distance_df.count())

long_distance_df.select(
    'trip_distance',
    'trip_duration_min',
    'fare_amount',
    'PULocationID',
    'DOLocationID'
).orderBy(
    F.col('trip_distance').desc()
).show(10, truncate=False)

print('LONG-DISTANCE TRIPS WITH AVERAGE SPEED:')
clean_df.filter(
    F.col('trip_distance') > 100
).select(
    'trip_distance',
    'trip_duration_min',
    'avg_speed_mph',
    'fare_amount'
).orderBy(
    F.col('trip_distance').desc()
).show(10, truncate=False)

print('SPEED OUTLIER COUNT:')
print(
    clean_df.filter(
        F.col('is_speed_outlier') == True
    ).count()
)

print('SPEED OUTLIER RECORDS:')
clean_df.filter(
    F.col('is_speed_outlier') == True
).select(
    'trip_distance',
    'trip_duration_min',
    'avg_speed_mph',
    'is_speed_outlier'
).orderBy(
    F.col('avg_speed_mph').desc()
).show(10, truncate=False)

print('TRIPS LONGER THAN 3 HOURS:')
print(long_duration_df.count())

print('LONGEST DURATION RECORDS:')
long_duration_df.select(
    'trip_duration_min',
    'trip_distance',
    'avg_speed_mph',
    'fare_amount',
    'PULocationID',
    'DOLocationID',
).orderBy(
    F.col('trip_duration_min').desc()
).show(10, truncate=False)


# Flag unusually long trip durations
clean_df = clean_df.withColumn(
    'is_duration_outlier',
    F.col('trip_duration_min') > 180
)

print("DURATION OUTLIER COUNT:")
print(
    clean_df.filter(
        F.col('is_duration_outlier') == True
    ).count()
)

print("DURATION OUTLIER RECORDS:")
clean_df.filter(
    F.col('is_duration_outlier') == True
).select(
    'trip_duration_min',
    'trip_distance',
    'avg_speed_mph',
    'is_duration_outlier'
).orderBy(
    F.col('trip_duration_min').desc()
).show(10, truncate=False)

SILVER_PATH = PROJECT_ROOT / 'data/silver/yellow_taxi_2024_01'
REJECTED_PATH = PROJECT_ROOT / 'data/rejected/yellow_taxi_2024_01'

clean_df.write.mode('overwrite').parquet(str(SILVER_PATH))

rejected_df.write.mode('overwrite').parquet(str(REJECTED_PATH))

print('Silver and rejected datasets saved successfully')
spark.stop()