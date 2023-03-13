# Importing required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.types import  StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType
from pyspark.sql.functions import expr, from_json, col, concat
from pyspark.sql import Window


# Defining the schema for invoice data
schema = StructType([
	StructField("invoice_no", StringType(), True),
	StructField("country", StringType(), True),
	StructField("timestamp", TimestampType(), True),
	StructField("total_cost", DoubleType(), True),
	StructField("total_items", IntegerType(), True),
	StructField("is_order",IntegerType() , True),
	StructField("is_return",IntegerType() , True)
	])


valueschema = StructType([
	
	StructField("invoice_no", StringType(), True),
	StructField("country", StringType(), True),
	StructField("timestamp", TimestampType(), True),
	StructField("type", StringType(), True),
	StructField("items", ArrayType(
					StructType([
								StructField("SKU", StringType()),
								StructField("title", StringType()),
								StructField("unit_price", DoubleType()),
							StructField("quantity", IntegerType())	
						])
				   )
				)

	])


# Defining user defined functions
def get_total_cost(val, typ):
	
	if type(val) == "<list>" and len(val) == 0: 
		return 0
	elif typ == None or typ == "None" or typ is None:
		return 0
	elif val == None or val == "None" or val is None:
		return 0		
	else:
		ans  = 0

		if typ.lower() == "order": 
			for x in val:
				if x["unit_price"] != None and x["unit_price"] != "None" and x["unit_price"] != "none" and x["unit_price"] != "null" \
					and x["quantity"] != None and x["quantity"] != "None" and x["quantity"] != "none" and x["quantity"] != "null":
					ans += float(x["unit_price"])*float(x["quantity"])   
		else:
			for x in val:
				if x["unit_price"] != None and x["unit_price"] != "None" and x["unit_price"] != "none" and x["unit_price"] != "null" \
					and x["quantity"] != None and x["quantity"] != "None" and x["quantity"] != "none" and x["quantity"] != "null":
					ans += -float(x["unit_price"])*float(x["quantity"])   
		
		return ans

	return 0	

def get_total_items(val):
	
	if type(val) == "<list>" and len(val) == 0: 
		return 0
	elif val == None or val == "None" or val is None:
		return 0		
	else:
		ans = 0
		ans = len(val)
		return ans
				 
	return 0


def get_order(typ):
	
	if typ == None or typ == "None" or typ is None:
		return 0
	else:	
		return 1 if typ.lower() == "order" else 0

def get_return(typ):
	if typ == None or typ == "None" or typ is None:
		return 0
	else:	
		return 0 if typ.lower() == "order" else 1

	
# Creating spark session
spark = SparkSession  \
	.builder  \
	.appName("StructuredSocketRead")  \
	.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Reading event stream from Kafka 
lines = spark  \
	.readStream  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers","18.211.252.152:9092")  \
	.option("subscribe","real-time-project")  \
	.load()


# Reading the Key-value pair data
kafkaDF = lines.selectExpr("cast(key as string)","cast(value as string)")

kafkaDF = kafkaDF.select(from_json(col("value"), valueschema).alias("data"))

total_cost_udf = udf(lambda t, y: get_total_cost(t, y), DoubleType())
total_items_udf = udf(lambda t: get_total_items(t), IntegerType())
is_order_udf = udf(lambda t: get_order(t), IntegerType())
is_return_udf = udf(lambda t: get_return(t), IntegerType())


# Defining the kafka data stream
kafkaDF = kafkaDF.withColumn("invoice_no", 	kafkaDF["data"]["invoice_no"]) \
				 .withColumn("country", 	kafkaDF["data"]["country"]) \
				 .withColumn("timestamp", 	kafkaDF["data"]["timestamp"]) \
				 .withColumn("total_cost",  total_cost_udf(kafkaDF["data"]["items"], kafkaDF["data"]["type"])) \
				 .withColumn("total_items", total_items_udf(kafkaDF["data"]["items"])) \
				 .withColumn("is_order",    is_order_udf(kafkaDF["data"]["type"])) \
				 .withColumn("is_return",   is_return_udf(kafkaDF["data"]["type"]))\
				 .drop('data')


query = kafkaDF \
	.writeStream \
	.outputMode("append") \
	.format("console") \
	.start()


# Time basis query with watermark of 1 minute and window of 1 minute
kpitimebasis = kafkaDF.withWatermark("timestamp", "1 minute") \
					  .groupBy(window("timestamp", "1 minute", "1 minute")) \
					  .agg( 	sum("total_cost").alias("total_sale_volume"),
					  			count("invoice_no").alias("opm"),
					  			avg("is_return").alias("rate_of_return"),
					  			avg("total_cost").alias("average_transaction_size")
					  	  )

querytime = kpitimebasis.writeStream \
						.format("json") \
						.outputMode("append") \
						.option("truncate", "false") \
						.option("path", "time-wise-kpi1") \
						.option("checkpointLocation", "time-cp1") \
						.trigger(processingTime = "1 minute") \
						.start()
					

# Country-Time basis query with watermark of 1 minute and window of 1 minute

kpitimecountrybasis = kafkaDF.withWatermark("timestamp", "1 minute") \
					  .groupBy(window("timestamp", "1 minute", "1 minute"), "country") \
					  .agg( 	sum("total_cost").alias("total_sale_volume"),
					  			count("invoice_no").alias("opm"),
					  			avg("is_return").alias("rate_of_return")
					  	  )



querycountrytime = kpitimecountrybasis.writeStream \
						.format("json") \
						.outputMode("append") \
						.option("truncate", "false") \
						.option("path", "time-country-wise-kpi1") \
						.option("checkpointLocation", "time-country-cp1") \
						.trigger(processingTime = "1 minute") \
						.start()
					


querycountrytime.awaitTermination()

