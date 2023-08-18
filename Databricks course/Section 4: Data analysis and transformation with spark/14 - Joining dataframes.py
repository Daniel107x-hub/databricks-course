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
display(countries_df)
display(regions_df)

# COMMAND ----------

from pyspark.pandas import DataFrame
employees = {
    "emp_id": [677509, 940761, 428945, 499687],
    "name": ['L Walker', 'B Robinson', 'J Robinson', 'P Bailey'],
    "dept_id": [2, 2, 1, 0],
    "salary": [51536, 40887, 50445, 61603]
}

employees_df = DataFrame(data=employees, columns=['emp_id', 'name', 'dept_id', 'salary'])
employees_df.display()

# COMMAND ----------

departments = {
    "id": [1, 2, 3, 4],
    "dept_name": ['Finance', 'Marketing', 'Production', 'Sales'],
    "dept_location": ['Europe', 'USA', 'Europe', 'USA']
}

dep_df = DataFrame(data=departments, columns=['id', 'dept_name', 'dept_location'])
dep_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inner join
# MAGIC

# COMMAND ----------

# Using a pandas Dataframe
employees_df.join(right=dep_df.set_index('id'), on='dept_id', how='inner').display()

# COMMAND ----------

# Using the countries data (SQL Dataframe)
countries_df.join(regions_df, countries_df['REGION_ID']==regions_df['ID'], 'inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Right join

# COMMAND ----------

# In a pandas dataframe
employees_df.join(right=dep_df.set_index('id'), on='dept_id', how='right').display()

# In a pyspark sql dataframe
countries_df.join(regions_df, countries_df['REGION_ID']==regions_df['ID'], 'right').select(countries_df['NAME'], countries_df['POPULATION'], regions_df['NAME']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Task
# MAGIC 1. Make an inner join con the countries and regions table.
# MAGIC 2. Display only region name, country name and population
# MAGIC 3. Alias country name as country_name and region name as region_name
# MAGIC 4. Sort the results indescencing population order
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import desc

countries_df.join(regions_df, countries_df['REGION_ID']==regions_df['ID'], 'inner')\
    .select(countries_df['NAME'].alias('country_name'), countries_df['population'], regions_df['NAME'].alias('region_name'))\
    .sort(desc(countries_df['population']))\
    .display()
