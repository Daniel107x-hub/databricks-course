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
countries_df = spark.read.csv(countries_path, schema=schema, header=True)
display(countries_df)

# COMMAND ----------

from pyspark.sql.functions import current_date
countries_df.withColumn('date', current_date()).display()

# COMMAND ----------

from pyspark.sql.functions import lit
countries_df.withColumn('updated_by', lit('Danie107')).display()

# COMMAND ----------

countries_df.withColumn('population_in_millions', countries_df['population']/1000000).display()

# COMMAND ----------


