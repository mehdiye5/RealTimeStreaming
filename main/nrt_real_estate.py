import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col,from_json
from pyspark.streaming import StreamingContext
from math import radians, cos, sin, asin, sqrt, degrees, atan2

# add more functions as necessary
def calculate_distance(la1, lo1, la2, lo2):
    R = 6373.0

    lat1 = radians(float(la1))
    lon1 = radians(float(lo1))
    lat2 = radians(float(la2))
    lon2 = radians(float(lo2))

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance

def main(host, topic_name, table_identifier):
    # main logic starts here
    df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", host) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", 'earliest') \
    .load()
    m_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    #json schema
    schema = types.StructType([
        types.StructField('zpid', types.StringType()),
        types.StructField('city', types.StringType()),
        types.StructField('streetAddress', types.StringType()),
        types.StructField('zipcode', types.StringType())
    ])
    # value:json column >> multiple column
    json_df = m_df.withColumn("json_data",from_json(col("value"),schema)).select("json_data.*")

    stream = json_df.writeStream.format('iceberg') \
        .outputMode('append') \
        .option("path", table_identifier) \
        .option("checkpointLocation", "checkpoint") \
        .start()

    stream.awaitTermination(30)
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