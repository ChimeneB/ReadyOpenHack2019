# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Data Engineering
# MAGIC 
# MAGIC When conforming this data for downstream consumption: 
# MAGIC - The team is expected to create and store unified sets of common elements which exist across all source systems (e.g., the set of movies)
# MAGIC - The team is not expected to design a schema in which transactions from different business models are combined; for example, teams should not attempt to map a Shipment - Date of a DVD sales order to the Return Date of a rental
# MAGIC 
# MAGIC Given the various downstream consumption patterns and user personas, e.g. business intelligence versus machine learning, the team should not apply any logical business rules at this time. 
# MAGIC - Creating a homogenous dataset with consistent column names and data types is common work which will benefit most, if not all, downstream consumers.
# MAGIC - Various downstream consumers may have different requirements when it comes to de-duplication, resolution of conflicts between source systems, etc. The team should not make such decisions now, as these downstream requirements have not yet been specified.

# COMMAND ----------

# MAGIC %fs ls mnt/data/Raw/

# COMMAND ----------

# DBTITLE 1,Load FourthCoffee
fc_actor = spark.read.csv("dbfs:/mnt/data/Raw/FourthCoffee/2019-07-15/Actors.csv", header ="true")
fc_customer = spark.read.csv("dbfs:/mnt/data/Raw/FourthCoffee/2019-07-15/Customers.csv", header ="true")
fc_movieactor = spark.read.csv("dbfs:/mnt/data/Raw/FourthCoffee/2019-07-15/MovieActors.csv", header ="true")
fc_movie = spark.read.csv("dbfs:/mnt/data/Raw/FourthCoffee/2019-07-15/Movies.csv", header ="true")
fc_onlinemoviemap = spark.read.csv("dbfs:/mnt/data/Raw/FourthCoffee/2019-07-15/OnlineMovieMappings.csv", header ="true")
#fc_actordelete = spark.read.csv("dbfs:/mnt/data/Raw/FourthCoffee/2019-07-15/actors_delete", header ="true")

# COMMAND ----------

# DBTITLE 1,Load VanArsdel
va_actor = spark.read.parquet("dbfs:/mnt/data/Raw/VanArsdel/2019-07-15/dbo_Actors")
va_customer = spark.read.parquet("dbfs:/mnt/data/Raw/VanArsdel/2019-07-15/dbo_Customers")
va_movieactor = spark.read.parquet("dbfs:/mnt/data/Raw/VanArsdel/2019-07-15/dbo_MovieActors")
va_movie = spark.read.parquet("dbfs:/mnt/data/Raw/VanArsdel/2019-07-15/dbo_Movies")
va_onlinemoviemap = spark.read.parquet("dbfs:/mnt/data/Raw/VanArsdel/2019-07-15/dbo_OnlineMovieMappings")
va_transactions = spark.read.parquet("dbfs:/mnt/data/Raw/VanArsdel/2019-07-15/dbo_Transactions")

# COMMAND ----------

# DBTITLE 1,Load Southridge
sr_movies = spark.read.json("dbfs:/mnt/data/Raw/Southridge/2019-07-15/SouthridgeMovies.JSON") #CosmosDB
sr_addresses = spark.read.parquet("dbfs:/mnt/data/Raw/Southridge/2019-07-15/dbo_Addresses")
sr_customers = spark.read.parquet("dbfs:/mnt/data/Raw/Southridge/2019-07-15/dbo_Customers")
sr_orderdetails = spark.read.parquet("dbfs:/mnt/data/Raw/Southridge/2019-07-15/dbo_OrderDetails")
sr_orders = spark.read.parquet("dbfs:/mnt/data/Raw/Southridge/2019-07-15/dbo_Orders")

# COMMAND ----------

# DBTITLE 1,Load Customer data
fc_customer = spark.read.csv("dbfs:/mnt/data/Raw/FourthCoffee/2019-07-15/Customers.csv", header ="true")
va_customer = spark.read.parquet("dbfs:/mnt/data/Raw/VanArsdel/2019-07-15/dbo_Customers")
sr_customer = spark.read.parquet("dbfs:/mnt/data/Raw/Southridge/2019-07-15/dbo_Customers")

# COMMAND ----------

# DBTITLE 1,Create a "SourceSystemId" unique ID as a string datatype
fc_customer = fc_customer.withColumn("SourceSystemId", fc_customer["CustomerID"])
va_customer = va_customer.withColumn("SourceSystemId", va_customer["CustomerID"])
sr_customer = sr_customer.withColumn("SourceSystemId", sr_customer["CustomerID"])

# COMMAND ----------

# DBTITLE 1, Rename "CustomerID" as "SourceSystemCustomerId"
fc_customer = fc_customer.withColumnRenamed("CustomerID", "SourceSystemCustomerId")
va_customer = va_customer.withColumnRenamed("CustomerID", "SourceSystemCustomerId")
sr_customer = sr_customer.withColumnRenamed("CustomerID", "SourceSystemCustomerId")

# COMMAND ----------

# DBTITLE 1,Create a "CustomerID" alternate key
#from pyspark.sql.functions import regexp_extract, col

#fc_customer = fc_customer.withColumn("CustomerId", regexp_extract("SourceSystemCustomerId", '(\d+)', 1))
#va_customer = va_customer.withColumn("CustomerId", regexp_extract("SourceSystemCustomerId", '(\d+)', 1))
#sr_customer = sr_customer.withColumn("CustomerId", regexp_extract("SourceSystemCustomerId", '(\d+)', 1))

fc_customer = fc_customer.withColumn("CustomerID", fc_customer["SourceSystemCustomerId"])
va_customer = va_customer.withColumn("CustomerID", va_customer["SourceSystemCustomerId"])
sr_customer = sr_customer.withColumn("CustomerID", sr_customer["SourceSystemCustomerId"])

# COMMAND ----------

# DBTITLE 1,Convert FourthCoffee "CreatedDate"  & "UpdatedDate" to date datatype
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import to_date

fc_customer = fc_customer.withColumn("CreatedDate", to_date(unix_timestamp("CreatedDate", "yyyy-MM-dd").cast("timestamp")))
fc_customer = fc_customer.withColumn("UpdatedDate", to_date(unix_timestamp("UpdatedDate", "yyyy-MM-dd").cast("timestamp")))

# COMMAND ----------

# DBTITLE 1,Convert FourthCoffee "ZipCode" to a string datatype
fc_customer.withColumn("ZipCode", fc_customer["ZipCode"].cast("string")) 

# COMMAND ----------

# DBTITLE 1,Convert "CustomerId" & "PhoneNumber" to a Long data type
from pyspark.sql.types import IntegerType

fc_customer = fc_customer.withColumn("CustomerId", fc_customer["CustomerId"].cast(IntegerType()))
va_customer = va_customer.withColumn("CustomerId", va_customer["CustomerId"].cast(IntegerType()))
sr_customer = sr_customer.withColumn("CustomerId", sr_customer["CustomerId"].cast(IntegerType()))

fc_customer = fc_customer.withColumn("PhoneNumber", fc_customer["PhoneNumber"].cast(IntegerType()))
va_customer = va_customer.withColumn("PhoneNumber", va_customer["PhoneNumber"].cast(IntegerType()))
sr_customer = sr_customer.withColumn("PhoneNumber", sr_customer["PhoneNumber"].cast(IntegerType()))

# COMMAND ----------

# DBTITLE 1,Rearrange Columns
fc_customer = fc_customer[['SourceSystemId', 'CustomerId', 'SourceSystemCustomerId', 'LastName', 'FirstName', 'PhoneNumber', 'CreatedDate', 'UpdatedDate', 'AddressLine1', 'AddressLine2', 'City', 'State', 'ZipCode']]
va_customer = fc_customer[['SourceSystemId', 'CustomerId', 'SourceSystemCustomerId', 'LastName', 'FirstName', 'PhoneNumber', 'CreatedDate', 'UpdatedDate', 'AddressLine1', 'AddressLine2', 'City', 'State', 'ZipCode']]
sr_customer = fc_customer[['SourceSystemId', 'CustomerId', 'SourceSystemCustomerId', 'LastName', 'FirstName', 'PhoneNumber', 'CreatedDate', 'UpdatedDate', 'AddressLine1', 'AddressLine2', 'City', 'State', 'ZipCode']]

# COMMAND ----------

# DBTITLE 1,Join the files together
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame

def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

customer = unionAll(fc_customer, va_customer, sr_customer)

# COMMAND ----------

# DBTITLE 1,Write to ADLS Gen2 under the Curated folder as Parquet files
customer.write.mode("overwrite").parquet("/mnt/data/Curated/Customers/2019-07-15/")

# COMMAND ----------

# DBTITLE 1,Check the file write
# MAGIC %fs ls mnt/data/Curated/Customers/2019-07-15/

# COMMAND ----------

#dbutils.fs.rm("/mnt/data/Curated/Customers/", True)