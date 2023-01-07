import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col,from_json
from pyspark.streaming import StreamingContext
from math import radians, cos, sin, asin, sqrt, degrees, atan2


def main(host, topic_name, table_identifier):

    conf = SparkConf()
    conf.setMaster("local").setAppName("Iceberg Test")
    conf.setAll([('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'), ('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog'), ('catalog.spark_catalog.type', 'hive'), ('spark.sql.catalog.local', 'org.apache.iceberg.spark.SparkCatalog'), ('spark.sql.catalog.local.type', 'hadoop'), ('spark.sql.catalog.local.warehouse', 'warehouse')])
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    # spark.sql("DROP TABLE IF EXISTS local.db.viewedtable;")
    # spark.sql("CREATE TABLE local.db.viewedtable (tenantid string, city string, zpid string, garageSpaces integer, hasHeating boolean, latitude double, longitude double, hasView boolean, homeType string, parkingSpaces integer, yearBuilt integer, latestPrice double, lotSizeSqFt double, livingAreaSqFt double, numOfPrimarySchools integer, numOfElementarySchools integer, numOfMiddleSchools integer, numOfHighSchools integer, avgSchoolDistance double, avgSchoolRating double, numOfBathrooms double, numOfBedrooms double, numOfStories integer, timestamp timestamp) USING iceberg partitioned by (tenantid);")

    # main logic starts here
    df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", host) \
    .option("subscribe", topic_name) \
    .load()
    m_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")

    #json schema
    schema = types.StructType([
        types.StructField('city', types.StringType()),
        types.StructField('zpid', types.StringType()),
        types.StructField('garageSpaces', types.IntegerType()),
        types.StructField('hasHeating', types.BooleanType()),
        types.StructField('latitude', types.DoubleType()),
        types.StructField('longitude', types.DoubleType()),
        types.StructField('hasView', types.BooleanType()),
        types.StructField('homeType', types.StringType()),
        types.StructField('parkingSpaces', types.IntegerType()),
        types.StructField('yearBuilt', types.IntegerType()),
        types.StructField('latestPrice', types.DoubleType()),
        types.StructField('lotSizeSqFt', types.DoubleType()),
        types.StructField('livingAreaSqFt', types.DoubleType()),
        types.StructField('numOfPrimarySchools', types.IntegerType()),
        types.StructField('numOfElementarySchools', types.IntegerType()),
        types.StructField('numOfMiddleSchools', types.IntegerType()),
        types.StructField('numOfHighSchools', types.IntegerType()),
        types.StructField('avgSchoolDistance', types.DoubleType()),
        types.StructField('avgSchoolRating', types.DoubleType()),
        types.StructField('numOfBathrooms', types.DoubleType()),
        types.StructField('numOfBedrooms', types.DoubleType()),
        types.StructField('numOfStories', types.IntegerType()),
    ])

    # value:json column >> multiple column
    json_df = m_df.withColumn("json_data",from_json(col("value"),schema)).select(col("key").alias("tenantid"),"json_data.*", "timestamp")


    stream = json_df.writeStream.format('iceberg') \
        .outputMode('append') \
        .option("path", table_identifier) \
        .option("checkpointLocation", "checkpoint") \
        .option("failOnDataLoss", "false") \
        .start()


    stream.awaitTermination(30)

    return 


if __name__ == '__main__':
    host = sys.argv[1]
    topic_name = sys.argv[2]
    table_identifier = sys.argv[3]

    main(host, topic_name, table_identifier)