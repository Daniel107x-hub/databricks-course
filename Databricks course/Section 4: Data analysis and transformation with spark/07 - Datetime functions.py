# Databricks notebook source
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructType, StructField
countries_path = '/FileStore/tables/countries.csv'
schema = StructType([
    StructField('COUNTRY_ID', IntegerType(), False), 
    StructField('NAME', StringType(), False),
    StructField('NATIONALITY', StringType(), False),
    StructField('COUNTRY_CODE', StringType(), False),
    StructField('ISO_ALPHA2', StringType(), False),
    StructField('CAPITAL', StringType(), False),
    StructField('POPULATION', IntegerType(), False),
    StructField('AREA_KM2', DoubleType(), False),
    StructField('REGION_ID', IntegerType(), True),
    StructField('SUB_REGION_ID', IntegerType(), True),
    StructField('INTERMEDIATE_REGION_ID', IntegerType(), True),
    StructField('ORGANIZATION_REGION_ID', IntegerType(), True)
])
countries_df = spark.read.csv(countries_path, header=True, schema=schema)
display(countries_df)

# COMMAND ----------

from pyspark.sql.functions import *
countries_df = countries_df.withColumn('time', current_timestamp())
display(countries_df)

# COMMAND ----------

countries_df.select(month(countries_df['time'])).display()

# COMMAND ----------

countries_df = countries_df.withColumn('date_literal', lit('27-10-2020'))
display(countries_df)
countries_df.dtypes

# COMMAND ----------

countries_df = countries_df.withColumn('date_literal', to_date(countries_df['date_literal'], 'dd-MM-yyyy'))
display(countries_df)
countries_df.dtypes
