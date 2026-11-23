from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_PATH = PROJECT_ROOT / 'data/silver/yellow_taxi_2024_01'
ZONE_PATH = PROJECT_ROOT / 'data/reference/taxi_zone_lookup.csv'

spark = (
    SparkSession.builder
    .appName('NYC Taxi Zone Enrichment')
    .master('local[*]')
    .getOrCreate()
)

taxi_df = spark.read.parquet(str(SILVER_PATH))

zones_df = spark.read.csv(
    str(ZONE_PATH),
    header = True,
    inferSchema = True
)

print('TAXI ZONE LOOKUP:')
zones_df.show(10, truncate=False)

print('TAXI ZONE SCHEMA:')
zones_df.printSchema()

pickup_zones_df = zones_df.select(
    F.col('LocationID').alias('pickup_location_id'),
    F.col('Borough').alias('pickup_borough'),
    F.col('Zone').alias('pickup_zone')
)

enriched_df = taxi_df.join(
    pickup_zones_df,
    taxi_df.PULocationID == pickup_zones_df.pickup_location_id,
    'left'
)

print('ENRICHED PICKUP RECORDS:')
enriched_df.select(
    'PULocationID',
    'pickup_borough',
    'pickup_zone',
    'trip_distance'
).show(10, truncate=False)

dropoff_zones_df = zones_df.select(
    F.col('LocationID').alias('dropoff_location_id'),
    F.col('Borough').alias('dropoff_borough'),
    F.col('Zone').alias('dropoff_zone')
)

enriched_df = enriched_df.join(
    dropoff_zones_df,
    enriched_df.DOLocationID == dropoff_zones_df.dropoff_location_id,
    'left'
)

print('ENRICHED PICKUP AND DROPOFF RECORDS:')
enriched_df.select(
    'PULocationID',
    'pickup_borough',
    'pickup_zone',
    'DOLocationID',
    'dropoff_borough',
    'dropoff_zone'
).show(10, truncate=False)

print('ORIGINAL SILVER RECORD COUNT:')
print(taxi_df.count())

print('ENRICHED RECORD COUNT:')
print(enriched_df.count())

print('UNMATCHED PICKUP LOCATIONS:')
print(
    enriched_df.filter(
        F.col('pickup_zone').isNull()
    ).count()
)

print('UNMATCHED DROPOFF LOCATIONS:')
print(
    enriched_df.filter(
        F.col('dropoff_zone').isNull()
    ).count()
)

enriched_df = enriched_df.drop(
    'pickup_location_id',
    'dropoff_location_id'
)

print('FINAL ENRICHED SCHEMA:')
enriched_df.printSchema()

ENRICHED_PATH = PROJECT_ROOT / 'data/silver/yellow_taxi_enriched_2024_01'

enriched_df.write.mode('overwrite').parquet(str(ENRICHED_PATH))

print('ENRICHED DATASET SAVED SUCCESSFULLY!')

saved_df = spark.read.parquet(str(ENRICHED_PATH))

print('SAVED ENRICHED RECORD COUNT:')
print(saved_df.count())

print('SAVED ENRICHED SCHEMA:')
print(saved_df.printSchema())
spark.stop()