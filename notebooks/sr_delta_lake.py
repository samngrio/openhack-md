# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.srlakedev.dfs.core.windows.net",
    "7cf10H/2YPrS90DD4YOGkws4XnaqdjI2odoKGEldrekK9DBZ9V7Mh7INgcOKSrz45/PHeJ6XDwAh+ASt2wbSwg==")

# COMMAND ----------

dbutils.fs.ls("abfss://sr-blob-container-dev@srlakedev.dfs.core.windows.net/")

# COMMAND ----------

def load(db, tables):
    db_ = db.replace('-', '_')
    spark.sql(f'create database {db_}')
    for table_name in tables:
        file = f'abfss://sr-blob-container-dev@srlakedev.dfs.core.windows.net/raw/{db}/{table_name}'
        df = spark.read.parquet(file)
        df.write.saveAsTable(f'{db_}.{table_name}')

# COMMAND ----------

load('cloud-sales', ['dboAddresses', 'dboCustomers', 'dboOrderDetails', 'dboOrders'])

# COMMAND ----------

load('cloud-streaming', ['dboAddresses', 'dboCustomers', 'dboTransactions'])

# COMMAND ----------

# MAGIC %sql
# MAGIC create temporary view movies_view as 
# MAGIC select * from json.`abfss://sr-blob-container-dev@srlakedev.dfs.core.windows.net/raw/movies/`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table movies.movies_items as
# MAGIC select * from movies_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movies.movies_items;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended cloud_sales.dboorderdetails

# COMMAND ----------

order_details_sources = {
    'Southridge': spark.table('cloud_sales.dboorderdetails')
}
order_sources = {
    'Southridge': spark.table('cloud_sales.dboorders')
}

# COMMAND ----------

orders.dtypes

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fc_transactions

# COMMAND ----------

order_details.dtypes

# COMMAND ----------

from pyspark.sql.functions import concat
from pyspark.sql.functions import lit

# COMMAND ----------

# order_details = order_details.withColumn('Quantity', order_details['Quantity'].cast('int'))

source_ids = {
    'Southridge': 1,
    'VanArsdel Ltd': 2,
    'Fourth Coffee': 3
}


def unify_order_details(source_dfs: dict):
    result = None
    for source_name in source_dfs.keys():
        source_id = source_ids[source_name]
        order_details = source_dfs[source_name]
        order_details = order_details.withColumn('Unit Cost', order_details['UnitCost'].cast('float'))
        order_details = order_details.withColumn('Line Number', order_details['LineNumber'].cast('int'))
        order_details = order_details.withColumn('UniqueOrderID', concat(lit(source_id), order_details['OrderDetailID']))
        order_details = order_details.withColumn('OrderDetailsID', order_details['OrderDetailID'])
        order_details = order_details.withColumn('UniqueMovieID', concat(lit(source_id), order_details['MovieID']))
        if result:
            result = result.union(result)
        else:
            result = order_details
    return result

# COMMAND ----------

from pyspark.sql.types import *
def unify_orders(source_dfs: dict):
    result = None
    for source_name in source_dfs.keys():
        source_id = source_ids[source_name]
        orders = source_dfs[source_name]
        #orders = orders.withColumn('OrderDate', orders['OrderDate'].cast(DateType()))
        orders = orders.withColumn('SourceID', concat(lit('cloud_sales')))
        orders = orders.withColumn('UniqueCustomerID', concat(lit('cloud_sales'), orders['CustomerID']))
        orders = orders.withColumn('TotalCost', orders['TotalCost'].cast('float'))
        if result:
            result = result.union(result)
        else:
            result = orders
    return result

# COMMAND ----------

orders = unify_orders(order_sources)
order_details = unify_order_details(order_details_sources)

# COMMAND ----------

sales = orders.join(order_details, orders.OrderID == order_details.OrderID).select(
    "SourceID",
    "UniqueOrderID",
    orders["OrderID"],
    "OrderDetailsID",
    "UniqueMovieID",
    "MovieID",
    "Quantity",
    "Unit Cost",
    "Line Number",
    "CustomerID",
    "UniqueCustomerID",
    "OrderDate",
    "ShipDate",
    "TotalCost"
)

# COMMAND ----------

sales.dtypes

# COMMAND ----------

display(sales)

# COMMAND ----------

# MAGIC %sql
# MAGIC create temporary view fc_transactions
# MAGIC using csv
# MAGIC options (
# MAGIC path = 'abfss://sr-blob-container-dev@srlakedev.dfs.core.windows.net/raw/fourth-coffee/Transactions.txt',
# MAGIC header = 'false'
# MAGIC );

# COMMAND ----------


