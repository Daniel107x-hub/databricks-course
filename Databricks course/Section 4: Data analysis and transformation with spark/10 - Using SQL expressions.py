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

from pyspark.sql.functions import expr


# COMMAND ----------

countries_df.display()

# COMMAND ----------

countries_df.select(expr("NAME as country_name")).display()

# COMMAND ----------

countries_df.select(expr("left(NAME, 2) as country_name")).display()

# COMMAND ----------

from pyspark.sql.functions import asc
countries_df.withColumn('population_class', expr("CASE WHEN population > 100000000 THEN 'Very large' WHEN population > 50000000 THEN 'Medium' ELSE 'Not large' END")).sort(asc(countries_df['population'])).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Agregar una nueva columna llamada area_class
# MAGIC Debe de tener los valores:
# MAGIC - large para entradas donde el area sea mayor a 1 millon
# MAGIC - medium di es mayor a 300000 pero menor o igual a 1 millon
# MAGIC - small en otro caso

# COMMAND ----------

# Using when function
from pyspark.sql.functions import when, asc
countries_df.withColumn('area_class', when(countries_df['area_km2'] > 1000000, 'large').when(countries_df['area_km2'] > 300000, 'medium').otherwise('small')).sort(asc(countries_df['area_km2'])).display()

# COMMAND ----------

# Using native SQL
countries_df.withColumn('area_class', expr("CASE WHEN area_km2 > 1000000 THEN 'large' WHEN area_km2 > 300000 THEN 'medium' ELSE 'small' END")).sort(expr("area_km2 asc")).display()

# COMMAND ----------


