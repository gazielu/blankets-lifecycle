# Databricks notebook source
# MAGIC %md
# MAGIC #Ingestion phase
# MAGIC Phase 1 - Connect to databases
# MAGIC 1. try JDBC uploaded to drivers
# MAGIC 2. try odbc

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install pyodbc
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# MAGIC apt-get update
# MAGIC ACCEPT_EULA=Y apt-get install msodbcsql17

# COMMAND ----------

#import pyodbc
source_server_name='gvs72076.inc.hpicorp.net'
source_db_name='faomrp'
source_username='SPACE_BI_IDS'
source_password='SPC-IDS!pswd'
source_tbl='SPACE_BI_IDS.ETL_TRG_CALC_LST'
#driver = 'ODBC Driver 17 for SQL Server'
sqlserver_url = 'jdbc:sqlserver://%s:2048;databaseName=%s' % (source_server_name, source_db_name)
source_df1 = spark.read.format("jdbc").option("url", sqlserver_url).option("dbtable", source_tbl).option("user", source_username).option("password", source_password).load()
display(source_df1)

# COMMAND ----------

#import pyodbc
source_server_name='gvs72069.inc.hpicorp.net'
source_db_name='GABI-P'
source_username='GABI_STG_RW_1'
source_password='fT7_E*4U6k_m'
source_tbl='admin_schema.etl_btch'
sqlserver_url = 'jdbc:sqlserver://%s:2048;databaseName=%s' % (source_server_name, source_db_name)
source_df = spark.read.format("jdbc").option("url", sqlserver_url).option("dbtable", source_tbl).option("user", source_username).option("password", source_password).load()


# COMMAND ----------

display(source_df)


# COMMAND ----------

#import pyodbc
source_server_name='172.20.206.100'
source_db_name='HUBBLE_DWH_ITG'
source_username='Tableau_itg'
source_password='Tab_itg4'
source_tbl='dbo.etl_log'
sqlserver_url = 'jdbc:sqlserver://%s:1433;databaseName=%s' % (source_server_name, source_db_name)
source_df = spark.read.format("jdbc").option("url", sqlserver_url).option("dbtable", source_tbl).option("user", source_username).option("password", source_password).load()


# COMMAND ----------

print("Count:",source_df.count())

# COMMAND ----------

source_server_name='172.20.206.100'
source_db_name='HUBBLE_DWH_ITG'
source_username='Tableau_itg'
source_password='Tab_itg4'
query='SELECT TOP (1000) [IP Address Of SQL Server] FROM [HUBBLE_DWH_ITG].[STG].[IP_Address]'
sqlserver_url = 'jdbc:sqlserver://%s:1433;databaseName=%s' % (source_server_name, source_db_name)
source_df = spark.read.format("jdbc").option("url", sqlserver_url).option("query", query).option("user", source_username).option("password", source_password).load()

# COMMAND ----------

display(source_df)
