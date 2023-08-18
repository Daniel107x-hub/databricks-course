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

# Countries cuya poblacion supera 1,000,000,000
countries_df.filter(countries_df['population'] > 1000000000).display()

# COMMAND ----------

# Countries cuya ciudad capital comienza con b
from pyspark.sql.functions import locate
countries_df.filter(locate('B', countries_df['capital']) == 1).display()

# COMMAND ----------

countries_df.filter((locate('B', countries_df['capital']) == 1) & (countries_df['population'] > 1000000000)).display()

# COMMAND ----------

countries_df.filter(countries_df['region_id'] == 10).display()

# COMMAND ----------

# Filtro usando sintaxis sql
countries_df.filter("region_id == 10").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Lista de records con el country name mayor a 15 caracteres y el region_id no es 10

# COMMAND ----------

 from pyspark.sql.functions import length
 countries_df.filter((length(countries_df['name']) > 15) & (countries_df['region_id'] != 10)).display()

# COMMAND ----------


