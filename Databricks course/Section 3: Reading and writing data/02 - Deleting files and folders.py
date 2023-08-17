# Databricks notebook source
dbutils.fs.help()

# COMMAND ----------

dbutils.fs.fsutils.rm('/FileStore/tables/countries-my-csv.csv', True) # Delete directory recursively

# COMMAND ----------

dbutils.fs.fsutils.rm('/FileStore/tables/countries_single_line.json')

# COMMAND ----------

dbutils.fs.fsutils.rm('/FileStore/tables/countries_multi_line.json')

# COMMAND ----------

dbutils.fs.fsutils.rm('/FileStore/tables/countries.txt')

# COMMAND ----------


