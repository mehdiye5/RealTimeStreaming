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


    # main logic starts here
    df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", host) \
    .option("subscribe", topic_name) \
    .load()
    m_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")

    #json schema
    json_schema = types.StructType([
        types.StructField('zpid', types.StringType()),
        types.StructField('city', types.StringType()),
        types.StructField('streetAddress', types.StringType()),
        types.StructField('zipcode', types.DoubleType()),
        types.StructField('description', types.StringType()),
        types.StructField('latitude', types.DoubleType()),
        types.StructField('longitude', types.DoubleType()),
        types.StructField('propertyTaxRate', types.DoubleType()),
        types.StructField('garageSpaces', types.IntegerType()),
        types.StructField('hasAssociation', types.BooleanType()),
        types.StructField('hasCooling', types.BooleanType()),
        types.StructField('hasGarage', types.BooleanType()),
        types.StructField('hasHeating', types.BooleanType()),
        types.StructField('hasSpa', types.BooleanType()),
        types.StructField('hasView', types.BooleanType()),
        types.StructField('homeType', types.StringType()),
        types.StructField('parkingSpaces', types.IntegerType()),
        types.StructField('yearBuilt', types.IntegerType()),
        types.StructField('latestPrice', types.DoubleType()),
        types.StructField('numPriceChanges', types.DateType()),
        types.StructField('latest_saledate', types.IntegerType()),
        types.StructField('latest_salemonth', types.IntegerType()),
        types.StructField('latest_saleyear', types.IntegerType()),
        types.StructField('latestPriceSource', types.StringType()),
        types.StructField('numOfPhotos', types.IntegerType()),
        types.StructField('numOfAccessibilityFeatures', types.IntegerType()),
        types.StructField('numOfAppliances', types.IntegerType()),
        types.StructField('numOfParkingFeatures', types.IntegerType()),
        types.StructField('numOfPatioAndPorchFeatures', types.IntegerType()),
        types.StructField('numOfSecurityFeatures', types.IntegerType()),
        types.StructField('numOfWaterfrontFeatures', types.IntegerType()),
        types.StructField('numOfWindowFeatures', types.IntegerType()),
        types.StructField('numOfCommunityFeatures', types.IntegerType()),
        types.StructField('lotSizeSqFt', types.DoubleType()),
        types.StructField('livingAreaSqFt', types.DoubleType()),
        types.StructField('numOfPrimarySchools', types.IntegerType()),
        types.StructField('numOfElementarySchools', types.IntegerType()),
        types.StructField('numOfMiddleSchools', types.IntegerType()),
        types.StructField('numOfHighSchools', types.IntegerType()),
        types.StructField('avgSchoolDistance', types.DoubleType()),
        types.StructField('avgSchoolRating', types.DoubleType()),
        types.StructField('avgSchoolSize', types.DoubleType()),
        types.StructField('MedianStudentsPerTeacher', types.DoubleType()),
        types.StructField('numOfBathrooms', types.DoubleType()),
        types.StructField('numOfBedrooms', types.DoubleType()),
        types.StructField('numOfStories', types.IntegerType()),
        types.StructField('homeImage', types.StringType()),

    ])

    # value:json column >> multiple column
    json_df = m_df.withColumn("json_data",from_json(col("value"),json_schema)).select("json_data.*")


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