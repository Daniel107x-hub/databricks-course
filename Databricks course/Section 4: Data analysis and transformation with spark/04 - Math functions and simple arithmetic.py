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

population = countries_df.select((countries_df['population']/1000000).alias('population_in_m'))
display(population)

# COMMAND ----------

from pyspark.sql.functions import round
population.select(round(population['population_in_m'], 3)).display()

# COMMAND ----------

countries_df.withColumn('population_m', round(countries_df['population']/1000000, 1)).display()

# COMMAND ----------


