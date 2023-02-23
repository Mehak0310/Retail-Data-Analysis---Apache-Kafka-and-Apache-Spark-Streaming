# Import the required dependencies
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName('RetailDataAnalysis').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Reading data from Kafka topic
invoices = spark  \
	.readStream  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers","18.211.252.152:9092")  \
	.option("subscribe","real-time-project")  \
	.load()

# Define custom schema for invoice
invoice_schema = StructType([StructField('invoice_no', LongType(), True),
                    StructField('country', StringType(), True),
                    StructField('timestamp', TimestampType(), True),
                    StructField('type', StringType(), True),
                    StructField('items',
                    ArrayType(StructType(
				[StructField('SKU', StringType(), True), 
				StructField('title',StringType(), True), 
				StructField('unit_price',FloatType(), True), 
				StructField('quantity',IntegerType(), True)])), True)])

# Create the Spark dataframe by parsing JSON as per the schema 
invoice_df = invoices.select(from_json(col('value').cast('string'),invoice_schema).alias('data')).select('data.*')

# Print schema of a single order
invoice_df.printSchema()

#### Utility methods ####

# Method to calculate the total cost for all items in an order
def total_order_cost(items, type):
    total_cost = 0
    for i in items:
        total_cost = i['unit_price'] * i['quantity'] + total_cost
    return (total_cost if type == 'ORDER' else total_cost * -1)

# Method to calculate the total number of items in an order
def total_order_items(items):
    total_items = 0
    for i in items:
        total_items = total_items + i['quantity']
    return total_items

# Method to check whether an order is a new order or not
def is_order(type):
    return (1 if type == 'ORDER' else 0)

# Method to check whether an order is a return order or not
def is_return(type):
    return (1 if type == 'RETURN' else 0)

# Converting the Python functions to UDF
total_order_cost = udf(total_order_cost, FloatType())
total_order_items = udf(total_order_items, IntegerType())
is_order = udf(is_order, IntegerType())
is_return = udf(is_return, IntegerType())

# Use the UDFs to create new columns 
summary_df = invoice_df.withColumn('total_cost', total_order_cost(invoice_df.items, invoice_df.type)) \
            .withColumn('total_items', total_order_items(invoice_df.items)) \
            .withColumn('is_order',is_order(invoice_df.type)) \
            .withColumn('is_return',is_return(invoice_df.type))  

# Write the summarised order values to the console
summaryQuery = summary_df.select(
    'invoice_no',
    'country',
    'timestamp',
    'total_cost',
    'total_items',
    'is_order',
    'is_return',
    ).writeStream \
       .outputMode("append") \
       .format("console") \
       .option("truncate", "false") \
       .trigger(processingTime="1 minute") \
       .start()

# Calculating Time-based KPIs
time_based_df = summary_df.withWatermark("timestamp", "1 minute") \
    .groupBy(window("timestamp","10 minutes","1 minute")) \
    .agg(count("invoice_no").alias("OPM"), \
         sum("total_cost").alias("total_sale_volume"), \
         avg("total_cost").alias("average_transaction_size"), \
         avg("is_return").alias("rate_of_return")) 

# Calculating time and country based KPIs
time_country_based_df = summary_df.withWatermark("timestamp", "1 minute") \
    .groupBy(window("timestamp","10 minutes","1 minute"), "country") \
    .agg(count("invoice_no").alias("OPM"), \
         sum("total_cost").alias("total_sale_volume"), \
         avg("is_return").alias("rate_of_return"))

# Writing time based KPIs to JSON Files
timeQuery = time_based_df.select("window.start","window.end","OPM","total_sale_volume","average_transaction_size","rate_of_return") \
    .writeStream \
    .outputMode("append") \
    .format("json") \
    .option("truncate", "false") \
    .option("path", "timeBasedKPI/") \
    .option("checkpointLocation", "timeBasedKPI_checkpoint/") \
    .trigger(processingTime="1 minute") \
    .start()

# Writing time and country based KPIs to JSON Files
timeCountryQuery = time_country_based_df.select("window.start","window.end","country","OPM","total_sale_volume","rate_of_return") \
    .writeStream \
    .outputMode("append") \
    .format("json") \
    .option("truncate", "false") \
    .option("path", "timeCountryBasedKPI/") \
    .option("checkpointLocation", "timeCountryBasedKPI_checkpoint/") \
    .trigger(processingTime="1 minute") \
    .start()

# Await termination
summaryQuery.awaitTermination()
timeQuery.awaitTermination()
timeCountryQuery.awaitTermination()
