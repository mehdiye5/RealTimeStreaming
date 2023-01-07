import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col,from_json
from pyspark.streaming import StreamingContext
from math import radians, cos, sin, asin, sqrt, degrees, atan2

# add more functions as necessary
schema = types.StructType([
    types.StructField('zpid', types.StringType()),
    types.StructField('city', types.StringType()),
    types.StructField('streetAddress', types.StringType()),
    types.StructField('zipcode', types.StringType()),
    types.StructField('description', types.StringType()),
    types.StructField('latitude', types.StringType()),
    types.StructField('longitude', types.StringType()),
    types.StructField('propertyTaxRate', types.StringType()),
    types.StructField('garageSpaces', types.IntegerType()),
    types.StructField('hasAssociation', types.StringType()),
    types.StructField('hasCooling', types.StringType()),
    types.StructField('hasGarage', types.StringType()),
    types.StructField('hasHeating', types.StringType()),
    types.StructField('hasSpa', types.StringType()),
    types.StructField('hasView', types.StringType()),
    types.StructField('homeType', types.StringType()),
    types.StructField('parkingSpaces', types.IntegerType()),
    types.StructField('yearBuilt', types.IntegerType()),
    types.StructField('latestPrice', types.StringType())
])

def main(host, topic_name, table_identifier):
    # main logic starts here
    df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", host) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", 'earliest') \
    .load()
    # .selectExpr("CAST(value AS STRING) as message") \
    # .select(from_json(functions.col("message"),schema).as("json")) \
    # .select("json.*")
    
    m_df = df.selectExpr("CAST(value AS STRING)")
    # value:json column >> multiple column
    json_df = m_df.withColumn("json_data",from_json(col("value"),schema)).select("json_data.*")

    stream = json_df.writeStream.format('console') \
        .outputMode('append') \
        .option("path", table_identifier) \
        .option("checkpointLocation", "checkpoint") \
        .start()

    stream.awaitTermination(200)
    return


if __name__ == '__main__':
    host = sys.argv[1]
    topic_name = sys.argv[2]
    table_identifier = sys.argv[3]
    conf = SparkConf()
    conf.setMaster("local").setAppName("Iceberg Test")
    conf.setAll([('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'), ('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog'), ('catalog.spark_catalog.type', 'hive'), ('spark.sql.catalog.local', 'org.apache.iceberg.spark.SparkCatalog'), ('spark.sql.catalog.local.type', 'hadoop'), ('spark.sql.catalog.local.warehouse', 'warehouse')])
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(host, topic_name, table_identifier)