# Databricks notebook source
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructType, StructField
countries_path = '/FileStore/tables/countries.csv'
regions_path = '/FileStore/tables/country_regions.csv'
countries_schema = StructType([
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
regions_schema = StructType([
    StructField('ID', IntegerType(), False),
    StructField('NAME', StringType(), False)
])
countries_df = spark.read.csv(countries_path, header=True, schema=countries_schema)
regions_df = spark.read.csv(regions_path, header=True, schema=regions_schema)

countries = countries_df.join(regions_df, countries_df['REGION_ID']==regions_df['ID'], 'inner')\
    .select(countries_df['NAME'].alias('country_name'), regions_df['NAME'].alias('region_name'), countries_df['population'])
display(countries)

# COMMAND ----------

pivoted_countries = countries.groupBy('country_name').pivot('region_name').sum('population')
pivoted_countries.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Y si, del dataframe anterior quisieramos crear una nueva columna llamada 'region', y las entradas en la matriz fueran otra columna llamada 'population'

# COMMAND ----------

from pyspark.sql.functions import expr

pivoted_countries.select('country_name', expr("stack(5, 'Africa', Africa, 'America', America, 'Asia', Asia, 'Europe', Europe, 'Oceania', Oceania) as (region_name, population)")).filter("population is not null").display()

# COMMAND ----------

countries_pd = countries_df.toPandas()
countries_pd
