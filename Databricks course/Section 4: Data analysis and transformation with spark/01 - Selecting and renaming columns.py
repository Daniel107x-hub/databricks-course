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

# COMMAND ----------

countries_df = spark.read.csv(countries_path, schema=schema, header=True)
display(countries_df)

# COMMAND ----------

# MAGIC %md
# MAGIC [Documentation about SQL for dataframes](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)

# COMMAND ----------

countries_df.select('name', 'capital', 'population').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Renaming columns
# MAGIC Renaming columns from a Column object
# MAGIC ```python
# MAGIC dataframe.select(dataframe.COL_1.alias('NAME'), dataframe.COL_2.alias('AGE'))
# MAGIC ```
# MAGIC

# COMMAND ----------

countries_df.select(countries_df['name'].alias('country_name'), countries_df['capital'].alias('capital_city'), countries_df['population']).display()

# COMMAND ----------

countries_df.select('name', 'capital', 'population').withColumnRenamed('name', 'country_name').withColumnRenamed('capital', 'capital_city').display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Exercise
# MAGIC Read the file country_regions y renmombrar la columna name a continent

# COMMAND ----------

regions_path = '/FileStore/tables/country_regions.csv'
regions_df = spark.read.csv(regions_path, header=True)
display(regions_df)
regions_df.dtypes

# COMMAND ----------

regions_schema = StructType([
    StructField('ID', IntegerType(), False),
    StructField('NAME', StringType(), False)
])
regions_df = spark.read.csv(regions_path, schema=regions_schema, header=True)
display(regions_df)
regions_df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC Renaming column NAME to continent

# COMMAND ----------

regions_df.select('*').withColumnRenamed('NAME', 'continent').display()

# COMMAND ----------

regions_df.select(regions_df['ID'].alias('region_id'), regions_df['NAME'].alias('continent')).display()
