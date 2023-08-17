# Databricks notebook source
# MAGIC %md
# MAGIC # Reading data from files

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading data from CSV File
# MAGIC [Dataframe reader for CSV](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.csv.html#pyspark.sql.DataFrameReader.csv)

# COMMAND ----------

countries = spark.read.csv('/FileStore/tables/countries.csv', header=True) # Extra option to take headers into account

# COMMAND ----------

type(countries)

# COMMAND ----------

display(countries)

# COMMAND ----------

countries_df = spark.read.options(header=True).csv('/FileStore/tables/countries.csv')l;

# COMMAND ----------

countries.dtypes

# COMMAND ----------

countries.schema

# COMMAND ----------

countries.describe

# COMMAND ----------

countries = spark.read.csv('/FileStore/tables/countries.csv', header=True) # Extr

# COMMAND ----------

countries = spark.read.csv('/FileStore/tables/countries.csv', header=True, inferSchema=True) # Permite que se infieran los tipos de datos

# COMMAND ----------

# MAGIC %md
# MAGIC En el caso de inferir los schemas, se generan 2 jobs de spark. El primero es para inferir los schemas y el segundo para leer los datos

# COMMAND ----------

countries.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC Especificacion del schema

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType
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

countries = spark.read.csv('/FileStore/tables/countries.csv', schema=schema, header=True)

# COMMAND ----------

display(countries)

# COMMAND ----------

countries.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading data from JSON file

# COMMAND ----------

countries_json = spark.read.json('/FileStore/tables/countries_single_line.json') # By default it reads json in a single line

# COMMAND ----------

display(countries_json)

# COMMAND ----------

countries_json.dtypes

# COMMAND ----------

countries_multiline_json = spark.read.json('/FileStore/tables/countries_multi_line.json', schema=schema, multiLine=True) # Enables multiline json reading

# COMMAND ----------

display(countries_multiline_json)

# COMMAND ----------

countries_multiline_json.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading tab separated text file

# COMMAND ----------

countries_text = spark.read.csv('/FileStore/tables/countries.txt', header=True, sep='\t')

# COMMAND ----------

display(countries_text)

# COMMAND ----------

# MAGIC %md
# MAGIC #Writing data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing data to CSV file

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType
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
countries_df = spark.read.json('/FileStore/tables/countries_single_line.json', schema=schema)
display(countries_df)

# COMMAND ----------

countries_df.write.csv('/FileStore/tables/countries-my-csv', header=True,  mode='overwrite') # El archivo se va a almacenar particionado debido al almacenamiento distribuido

# COMMAND ----------

countries_new_csv = spark.read.csv('/FileStore/tables/countries-my-csv', header=True) # Para leer el arvhico escrito basta con referenciar al folder que contiene todas las partes
display(countries_new_csv)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Partitioning data

# COMMAND ----------

countries_df.write.options(header=True).mode('overwrite').partitionBy('REGION_ID').csv('/FileStore/tables/countries-my-csv') # El archivo se va a almacenar particionado 

# COMMAND ----------

# MAGIC %md
# MAGIC Los datos ahora se han particionado por cada region

# COMMAND ----------

new_df = spark.read.csv('/FileStore/tables/countries-my-csv', header=True)
display(new_df)

# COMMAND ----------

# Reading a single region
new_df = spark.read.csv('/FileStore/tables/countries-my-csv/REGION_ID=10', header=True)
display(new_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Los datos se particionan en caso de que queramos hacer una mas rapida lectura, o para hacer procesamiento paralelo
