# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.srlakedev.dfs.core.windows.net",
    "7cf10H/2YPrS90DD4YOGkws4XnaqdjI2odoKGEldrekK9DBZ9V7Mh7INgcOKSrz45/PHeJ6XDwAh+ASt2wbSwg==")

# COMMAND ----------

dbutils.fs.ls("abfss://sr-blob-container-dev@srlakedev.dfs.core.windows.net/")

# COMMAND ----------

def load(db, tables):
    db_ = db.replace('-', '_')
#     spark.sql(f'create database {db_}')
    for table_name in tables:
        file = f'abfss://sr-blob-container-dev@srlakedev.dfs.core.windows.net/raw/{db}/{table_name}'
        df = spark.read.parquet(file)
        df.write.saveAsTable(f'{db_}.{table_name}')

# COMMAND ----------

load('cloud-sales', ['dboAddresses', 'dboCustomers', 'dboOrderDetails', 'dboOrders'])

# COMMAND ----------

load('cloud-streaming', ['dboAddresses', 'dboCustomers', 'dboTransactions'])

# COMMAND ----------

load('vanarsdel', ['dboActors', 'dboCustomers', 'dboMovieActors', 'dboMovies', 'dboOnlineMovieMappings'])

# COMMAND ----------

load('vanarsdel', ['dboTransactions'])

# COMMAND ----------

def load_fc(db, tables):
    db_ = db.replace('-', '_')
#     spark.sql(f'create database {db_}')
    for table_name in tables:
        file = f'abfss://sr-blob-container-dev@srlakedev.dfs.core.windows.net/raw/{db}/{table_name}.parquet'
        df = spark.read.parquet(file)
        df.write.saveAsTable(f'{db_}.{table_name}')

# COMMAND ----------

load_fc('fourth-coffee', ['Actors', 'Customers', 'MovieActors', 'Movies', 'OnlineMovieMappings', 'Transactions'])

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

order_details_sources['Southridge'].dtypes

# COMMAND ----------

order_sources['Southridge'].dtypes

# COMMAND ----------

from pyspark.sql.functions import concat
from pyspark.sql.functions import lit

# COMMAND ----------

source_ids = {
    'Southridge': 1,
    'VanArsdel Ltd': 2,
    'Fourth Coffee': 3
}

# COMMAND ----------

def unify_order_details(source_dfs: dict):
    result = None
    for source_name in source_dfs.keys():
        source_id = source_ids[source_name]
        order_details = source_dfs[source_name]
        order_details = order_details.withColumn('UnitCost', order_details['UnitCost'].cast('float'))
        order_details = order_details.withColumn('LineNumber', order_details['LineNumber'].cast('int'))
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
        orders = orders.withColumn('SourceID', lit(source_id))
        orders = orders.withColumn('UniqueCustomerID', concat(lit(source_id), orders['CustomerID']))
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
    "UnitCost",
    "LineNumber",
    "CustomerID",
    "UniqueCustomerID",
    "OrderDate",
    "ShipDate",
    "TotalCost"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database silver;

# COMMAND ----------

sales.write.saveAsTable('silver.sales')

# COMMAND ----------

sales.dtypes

# COMMAND ----------

display(sales)

# COMMAND ----------

spark.table('cloud_streaming.dbotransactions').dtypes

# COMMAND ----------

# MAGIC %sql
# MAGIC create temporary view transactions_csv as 
# MAGIC select * from text.`abfss://sr-blob-container-dev@srlakedev.dfs.core.windows.net/raw/fourth-coffee/Transactions.txt`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from transactions_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC create database fourth_coffee

# COMMAND ----------

spark.table('vanarsdel.dbomovies').dtypes

# COMMAND ----------

from pyspark.sql.types import *

movies_schema = StructType([
    StructField('MovieID', StringType(), False),
    StructField('MovieTitle', StringType(), False),
    StructField('Category', StringType(), False),
    StructField('Rating', StringType(), False),
    StructField('RunTimeMin', IntegerType(), False),
    StructField('ReleaseDate', StringType(), False)])

# COMMAND ----------

def read_csv(file, schema):
    return (spark.read
      .format("csv")
      .option("header", "false")
      .schema(schema)
      .load(f"abfss://sr-blob-container-dev@srlakedev.dfs.core.windows.net/raw/fourth-coffee/{file}.txt")
    )

# COMMAND ----------

fc_movies = read_csv('Movies', movies_schema)

# COMMAND ----------

display(fc_movies)

# COMMAND ----------

fc_movies.write.saveAsTable('fourth_coffee.movies')

# COMMAND ----------

# MAGIC %sql
# MAGIC create database fourth_coffee

# COMMAND ----------

fc_movies.write.saveAsTable('fourth_coffee.dbomovies')

# COMMAND ----------

spark.table('vanarsdel.dbocustomers').dtypes

# COMMAND ----------

customers_schema = StructType([
    StructField('CustomerID', StringType(), False),
    StructField('FirstName', StringType(), False),
    StructField('LastName', StringType(), False),
    StructField('AddressLine1', StringType(), False),
    StructField('AddressLine2', IntegerType(), False),
    StructField('City', StringType(), False),
    StructField('State', StringType(), False),
    StructField('ZipCode', StringType(), False),
    StructField('PhoneNumber', StringType(), False),
    StructField('CreatedDate', DateType(), False),
    StructField('UpdatedDate', DateType(), False),
])

# COMMAND ----------

fc_customers = read_csv('Customers', customers_schema)

# COMMAND ----------

fc_customers.display()

# COMMAND ----------

fc_customers.write.saveAsTable('fourth_coffee.dbocustomers')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fourth_coffee.transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vanarsdel.dbomovies

# COMMAND ----------

sb_catalog = spark.table('movies.movies_items')
va_movies = spark.table('vanarsdel.dbomovies')
va_actors = spark.table('vanarsdel.dboactors')
va_movieactors = spark.table('vanarsdel.dbomovieactors')
fc_movies = spark.table('fourth_coffee.movies')
fc_actors = spark.table('fourth_coffee.actors')
fc_movieactors = spark.table('fourth_coffee.movieactors')



# COMMAND ----------

from pyspark.sql.functions import explode
import uuid
import uuid
from pyspark.sql.functions import udf

uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())

actors_exploded = sb_catalog.select("*",explode(sb_catalog.actors).alias('actors_exp')).select('*', 'actors_exp.name')
actors_exploded = actors_exploded.withColumn("SourceID", lit(1))
actors_exploded = actors_exploded.withColumn("CatalogID",  uuidUdf())
actors_exploded = actors_exploded.withColumn("ActorID", lit(1))
actors_exploded = actors_exploded.withColumn("Actor", actors_exploded.name)
actors_exploded = actors_exploded.withColumn("AvailabilityYear", actors_exploded.releaseYear.cast('int'))
actors_exploded = actors_exploded.withColumn("ReleaseDate", actors_exploded.availabilityDate.cast('date'))
actors_exploded = actors_exploded.withColumn("ReleaseDate", actors_exploded.streamingAvailabilityDate.cast('date'))
actors_exploded = actors_exploded.withColumn("MovieTier", actors_exploded.tier)
actors_exploded = actors_exploded.withColumn("MovieTitle", actors_exploded.title)
actors_exploded = actors_exploded.withColumn("MovieID", actors_exploded.id)

# COMMAND ----------

sb_catalog = actors_exploded.select(
"SourceID",
"CatalogID",
"ActorID",
"Actor",
"ReleaseDate",
"Genre",
"Rating",
"AvailabilityYear",
"AvailabilityDate",
"MovieTier",
"MovieTitle",
"MovieID"
)

# COMMAND ----------

va_catalog = va_movies.join(va_movieactors, va_movies.MovieID == va_movieactors.MovieID).join(va_actors, va_movieactors.ActorID == va_actors.ActorID).select('va_movies')
va_catalog.display()


# COMMAND ----------

from pyspark.sql.functions import year
va_catalog = va_catalog.withColumn("SourceID",  lit(2))
va_catalog = va_catalog.withColumn("Genre",  lit("-"))
va_catalog = va_catalog.withColumn("MovieTier",  lit(None).cast(StringType()))
va_catalog = va_catalog.withColumn("CatalogID",  uuidUdf())
va_catalog = va_catalog.withColumn("Actor",  va_catalog.ActorName)
va_catalog = va_catalog.withColumn("AvailabilityYear",  year(va_catalog.ReleaseDate))
va_catalog = va_catalog.withColumn("AvailabilityDate",  va_catalog.ReleaseDate)


# COMMAND ----------


va_catalog = va_catalog.select(
"SourceID",
"CatalogID",   
va_actors["ActorID"],
"Actor",
"ReleaseDate",
"Genre",
"Rating",
"AvailabilityYear",
"AvailabilityDate",
"MovieTier",
"MovieTitle",
va_movies["MovieID"]
)

# COMMAND ----------

fc_catalog = fc_movies.join(fc_movieactors, fc_movies.MovieID == fc_movieactors.MovieID).join(fc_actors, fc_movieactors.ActorID == fc_actors.ActorID)
fc_catalog.display()


# COMMAND ----------

fc_catalog = fc_catalog.withColumn("SourceID",  lit(3))
fc_catalog = fc_catalog.withColumn("Genre",  lit("-"))
fc_catalog = fc_catalog.withColumn("MovieTier",  lit(None).cast(StringType()))
fc_catalog = fc_catalog.withColumn("CatalogID",  uuidUdf())
fc_catalog = fc_catalog.withColumn("Actor",  fc_catalog.ActorName)
fc_catalog = fc_catalog.withColumn("AvailabilityYear",  year(fc_catalog.ReleaseDate))
fc_catalog = fc_catalog.withColumn("AvailabilityDate",  fc_catalog.ReleaseDate)

# COMMAND ----------

fc_catalog = fc_catalog.select(
"SourceID",
"CatalogID",   
fc_actors["ActorID"],
"Actor",
"ReleaseDate",
"Genre",
"Rating",
"AvailabilityYear",
"AvailabilityDate",
"MovieTier",
"MovieTitle",
fc_movies["MovieID"]
)

# COMMAND ----------

fc_catalog.display()

# COMMAND ----------

full_catalog = sb_catalog.union(va_catalog).union(fc_catalog)

# COMMAND ----------

full_catalog.write.saveAsTable('silver.catalog')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.sales
