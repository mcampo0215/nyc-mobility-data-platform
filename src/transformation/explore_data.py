from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = PROJECT_ROOT / "data/raw/yellow_tripdata_2024-01.parquet"

#Connecting spark environment     
spark = (
    SparkSession.builder
    .appName("NYC Mobility Data Exploration")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.parquet(str(DATA_PATH))

print('DATASET SCHEMA:')
df.printSchema()

print('FIRST 5 RECORDS:')
df.show(5, truncate=False)

print('TOTAL RECORDS:')
print(df.count())

#Check missing values in important columns
print("MISSING VALUES:")

df.select([
    F.count(
        F.when(F.col(c).isNull(), 1)
    ).alias(c)
    for c in [
        'tpep_pickup_datetime',
        'tpep_dropoff_datetime',
        'passenger_count',
        'trip_distance',
        'fare_amount',
        'PULocationID',
        'DOLocationID'
    ]
]).show()

#check invalid or suspicious values
print('DATA QUALITY CHECKS:')

df.select(
    F.sum(F.when(F.col('fare_amount') < 0, 1).otherwise(0)).alias('negative_fares'),

    F.sum(F.when(
        F.col('tpep_dropoff_datetime') < F.col('tpep_pickup_datetime'), 1
    ).otherwise(0)).alias('negative_durations'),
).show()

print('DISTANCE QUALITY CHECKS:')

df.select(
    F.sum(F.when(F.col('trip_distance') < 0, 1).otherwise(0)).alias('negative_distances'),

    F.sum(F.when(F.col('trip_distance') == 0, 1).otherwise(0)).alias('zero_distances')
).show()

print('PASSENGER QUALITY CHECKS:')

df.select(
    F.sum(F.when(F.col('passenger_count') < 0, 1).otherwise(0)).alias('negative_passengers'),

    F.sum(F.when(F.col('passenger_count') == 0, 1).otherwise(0)).alias('zero_passengers')
).show()

# Examine numerical statistics
df.select(
    'trip_distance',
    'fare_amount',
    'total_amount',
    'passenger_count'
).describe().show()

#calculate duration in minutes
df = df.withColumn(
    'trip_duration_min',
    (
        F.unix_timestamp('tpep_dropoff_datetime') - F.unix_timestamp('tpep_pickup_datetime')
    ) / 60
)

print('DISTANCE OUTLIERS:')

df.select(
    F.sum(F.when(F.col('trip_distance') > 100, 1).otherwise(0)).alias('trips_over_100_miles'),

    F.sum(F.when(F.col('trip_distance') > 500, 1).otherwise(0)).alias('trips_over_500_miles')
).show()

print('DURATION OUTLIERS:')

df.select(
    F.sum(F.when(F.col('trip_duration_min') > 180, 1).otherwise(0)).alias('trips_over_3_hours'),

    F.sum(F.when(F.col('trip_duration_min') > 1440, 1).otherwise(0)).alias('trips_over_24_hours')
).show()

print('LONGEST RECORDED TRIPS:')

df.select(
    'trip_distance',
    'trip_duration_min',
    'fare_amount',
    'tpep_pickup_datetime',
    'tpep_dropoff_datetime'
).orderBy(
    F.col('trip_distance').desc()
).show(10, truncate=False)
spark.stop()