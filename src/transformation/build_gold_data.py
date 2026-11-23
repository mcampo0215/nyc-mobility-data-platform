from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]

ENRICHED_PATH = PROJECT_ROOT / 'data/silver/yellow_taxi_enriched_2024_01'

spark = (
    SparkSession.builder
    .appName('NYC Taxi Gold Analytics')
    .master('local[*]')
    .getOrCreate()
)

df = spark.read.parquet(str(ENRICHED_PATH))

print('ENRICHED RECORD COUNT:')
print(df.count())

borough_trips_df = (
    df.groupBy('pickup_borough')
    .agg(
        F.count('*').alias('total_trips'),
        F.avg('trip_distance').alias('avg_trip_distance'),
        F.expr('percentile_approx(trip_distance, 0.5)').alias('median_trip_distance'),

        F.avg(
            F.when(
                (F.col('trip_duration_min') > 0) &
                (F.col('avg_speed_mph') <= 100) &
                (F.col('trip_distance') >= 0),
                F.col('trip_distance')
            )
        ).alias('avg_valid_trip_distance'),

        F.count(
            F.when(
                (F.col('trip_duration_min') > 0) &
                (F.col('avg_speed_mph') <= 100) &
                (F.col('trip_distance') >= 0),
                1
            )
        ).alias('valid_distance_trips'),

        F.sum('total_amount').alias('total_revenue'),
        F.avg('total_amount').alias('avg_revenue_per_trip'),

        F.avg(
            F.when(
                F.col('trip_duration_min') > 0,
                F.col('trip_duration_min')
            )
        ).alias('avg_trip_duration_min'),

        F.expr(
            'percentile_approx(IF(trip_duration_min > 0, trip_duration_min, NULL), 0.5)'
        ).alias('median_trip_duration_min')
    )
    .orderBy(F.desc('total_trips'))
)

print('TOTAL TRIPS BY PICKUP BOROUGH:')
borough_trips_df.show(truncate=False)

print('TOP 10 LONGEST BROOKLYN PICKUP TRIPS:')

df.filter(
    F.col('pickup_borough') == 'Brooklyn'
).select(
    'pickup_zone',
    'dropoff_zone',
    'trip_distance',
    'trip_duration_min',
    'avg_speed_mph',
    'is_speed_outlier'
).orderBy(
    F.desc('trip_distance')
).show(10, truncate=False)

GOLD_PATH = PROJECT_ROOT / 'data/gold/borough_trip_summary_2024_01'
borough_trips_df.write.mode('overwrite').parquet(str(GOLD_PATH))
print('Gold borough summary saved successfully!')

gold_df = spark.read.parquet(str(GOLD_PATH))
print('GOLD DATASET RECORD COUNT:')
print(gold_df.count())

print('GOLD DATASET SCHEMA:')
print(gold_df.printSchema())
spark.stop()