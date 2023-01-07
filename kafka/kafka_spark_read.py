import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.streaming import StreamingContext
import re, math
from pyspark.sql.functions import col,from_json
# from write_kafka import send_message

def main(host, topic_name):

    # print topic records first,then read real time kafka streaming
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
        types.StructField('zipcode', types.StringType()),
        types.StructField('test', types.StringType()),
    ])
    # value:json column >> multiple column
    json_df = m_df.withColumn("json_data",from_json(col("value"),schema)).select('key',"json_data.*")

    stream = json_df.writeStream.format('console') \
        .outputMode('update').start()

    stream.awaitTermination(30)



if __name__ == '__main__':
    host = sys.argv[1]
    topic_name = sys.argv[2]
    spark = SparkSession.builder.appName('Spark kafka').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(host, topic_name)