# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Mount Azure Data Lake Store Gen 2
# MAGIC 
# MAGIC Configure a Service Principle. Please read how to use the portal to create an Azure AD application and [service principal](https://docs.microsoft.com/en-us/azure/active-directory/develop/app-objects-and-service-principals) that can access resources [here](https://docs.microsoft.com/en-nz/azure/active-directory/develop/howto-create-service-principal-portal).</br> 
# MAGIC 
# MAGIC Overall instructions [here](https://docs.databricks.com/spark/latest/data-sources/azure/azure-datalake-gen2.html).

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "9ef49bbe-f921-4303-ad0c-d4a101deb0f7",
           "fs.azure.account.oauth2.client.secret": "JArp+ze]k-60XpZLX11nv=7X8-VN1TW-",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/e5ff0039-b551-41b2-90e1-e230cc49eeae/oauth2/token"}

# COMMAND ----------

#Add <your-directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://data@stgdatalake.dfs.core.windows.net/",
  mount_point = "/mnt/data",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls mnt/data/Raw/FourthCoffee/2019-07-15/

# COMMAND ----------

df = spark.read.csv("dbfs:/mnt/data/Raw/FourthCoffee/2019-07-15/Actors.csv", header = "True")

# COMMAND ----------

display(df)