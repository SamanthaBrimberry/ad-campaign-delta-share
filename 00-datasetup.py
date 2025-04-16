# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta
from pyspark.sql.functions import *

# COMMAND ----------

catalog = 'sb'
schema = 'ads'
dbutils.widgets.text('catalog', catalog)
dbutils.widgets.text('schema', schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists ${catalog};
# MAGIC use catalog ${catalog};
# MAGIC create schema if not exists ${schema};

# COMMAND ----------

fake = Faker()

random.seed(42)
fake.seed_instance(42)

num_viewers = 100000
num_ads = 500
num_campaigns = 100
num_exposures = 50000
num_conversions = 5000

def random_date(start_date, end_date):
    return start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))

start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 8, 24)

# 1. Ad Campaign Data
campaigns = pd.DataFrame({
    'campaign_id': [f'CAMP{i:03d}' for i in range(1, num_campaigns + 1)],
    'advertiser': [fake.company() for _ in range(num_campaigns)],
    'start_date': [random_date(start_date, end_date) for _ in range(num_campaigns)],
    'end_date': [random_date(start_date, end_date) for _ in range(num_campaigns)],
    'budget': [random.randint(10000, 1000000) for _ in range(num_campaigns)]
})

# 2. Ad Data
ads = pd.DataFrame({
    'ad_id': [f'AD{i:04d}' for i in range(1, num_ads + 1)],
    'campaign_id': [random.choice(campaigns['campaign_id'].tolist()) for _ in range(num_ads)],
    'ad_format': [random.choice(['pre-roll', 'mid-roll', 'banner']) for _ in range(num_ads)],
    'ad_duration': [random.choice([15, 30, 60]) for _ in range(num_ads)]
})

# 3. Viewer Data
viewers = pd.DataFrame({
    'viewer_id': [f'VIEW{i:05d}' for i in range(1, num_viewers + 1)],
    'age': [random.randint(18, 80) for _ in range(num_viewers)],
    'gender': [random.choice(['M', 'F', 'Other']) for _ in range(num_viewers)],
    'location': [fake.city() for _ in range(num_viewers)]
})

# 4. Ad Exposure Data
exposures = pd.DataFrame({
    'exposure_id': [f'EXP{i:06d}' for i in range(1, num_exposures + 1)],
    'ad_id': [random.choice(ads['ad_id'].tolist()) for _ in range(num_exposures)],
    'viewer_id': [random.choice(viewers['viewer_id'].tolist()) for _ in range(num_exposures)],
    'timestamp': [random_date(start_date, end_date) for _ in range(num_exposures)],
    'device_type': [random.choice(['mobile', 'tablet', 'smart_tv', 'desktop']) for _ in range(num_exposures)],
    'content': [fake.catch_phrase() for _ in range(num_exposures)]
})

# 5. Conversion Data
conversions = pd.DataFrame({
    'conversion_id': [f'CONV{i:06d}' for i in range(1, num_conversions + 1)],
    'viewer_id': [random.choice(viewers['viewer_id'].tolist()) for _ in range(num_conversions)],
    'conversion_type': [random.choice(['subscription', 'app_install', 'content_view']) for _ in range(num_conversions)],
    'timestamp': [random_date(start_date, end_date) for _ in range(num_conversions)],
    'value': [random.uniform(0, 100) for _ in range(num_conversions)]
})

# 6. Attribution Data
attributions = pd.DataFrame({
    'attribution_id': [f'ATTR{i:06d}' for i in range(1, num_conversions + 1)],
    'conversion_id': conversions['conversion_id'],
    'exposure_id': [random.choice(exposures['exposure_id'].tolist()) for _ in range(num_conversions)],
    'attribution_model': [random.choice(['last_touch', 'first_touch', 'linear', 'time_decay']) for _ in range(num_conversions)],
    'attribution_weight': [random.uniform(0, 1) for _ in range(num_conversions)]
})

# COMMAND ----------

to_write = [
    ('campaigns', campaigns),
    ('ads', ads),
    ('viewers', viewers),
    ('exposures', exposures),
    ('conversions', conversions),
    ('attributions', attributions)
]

for table_name, data in to_write:
    (spark.createDataFrame(data)
        .write
        .mode('overwrite')
        .saveAsTable(f'{catalog}.{schema}.{table_name}')
    )

# COMMAND ----------

for x, i in to_write:
  print(f'{x}:')
  display(i)

# COMMAND ----------

# Ensure all DataFrames are Spark DataFrames
# exposures = spark.createDataFrame(exposures)
# viewers = spark.createDataFrame(viewers)
# ads = spark.createDataFrame(ads)
# campaigns = spark.createDataFrame(campaigns)

campaign_raw = (
  exposures
  .join(viewers.withColumn("viewer_id", col("viewer_id").cast("string")), on='viewer_id')
  .join(ads.withColumn("ad_id", col("ad_id").cast("string")), on='ad_id')
  .join(campaigns.withColumn("campaign_id", col("campaign_id").cast("string")), on='campaign_id')
  .withColumnRenamed("start_date", "campaign_start_date")
  .withColumnRenamed("end_date", "campaign_end_date")
  .withColumnRenamed("timestamp", "exposure_timestamp")
)

# campaign_raw.write.mode('overwrite').format('delta').saveAsTable(f"{catalog}.{schema}.campaign_raw")

# COMMAND ----------

campaigns_KPIs = campaign_raw.groupBy('campaign_id').agg(
    countDistinct('viewer_id').alias("total_viewers"),
    count("exposure_id").alias("total_exposures"),
    first("budget").alias("total_budget"),
    avg('age').alias("avg_age"),
    expr("percentile_approx(age, 0.5)").alias("median_age"),
    expr("percentile_approx(age, array(0.25, 0.75))").alias("age_quartiles")
)


(campaigns_KPIs.write
 .mode('overwrite')
 .format('delta')
 .saveAsTable(f'{catalog}.{schema}.campaign_KPIs')
)