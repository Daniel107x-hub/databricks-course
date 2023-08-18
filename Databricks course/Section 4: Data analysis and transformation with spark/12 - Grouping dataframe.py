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

# Suma de la poblacion por region
from pyspark.sql.functions import sum
countries_df.groupBy('region_id').sum('population').display()

# COMMAND ----------

# Obtener la poblacion minima por region
from pyspark.sql.functions import min, asc
countries_df.groupBy('region_id').sum('population', 'area_km2').display()

# COMMAND ----------

from pyspark.sql.functions import avg
countries_df.groupBy('region_id').agg(avg('population'), sum('area_km2')).display()

# COMMAND ----------

# Agregando y renombrando resultados
from pyspark.sql.functions import asc

countries_df.groupBy('region_id', 'sub_region_id')\
.agg(sum('population'), sum('area_km2'))\
.withColumnRenamed('sum(population)', 'total_pop')\
.withColumnRenamed('sum(area_km2)', 'total_area')\
.display()

# COMMAND ----------


