

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col,from_json, to_json
from pyspark.streaming import StreamingContext
from math import radians, cos, sin, asin, sqrt, degrees, atan2


import do_recommendation

from kafka import KafkaProducer
from kafka import KafkaConsumer

def update_viewedtable(table_identifier, my_tenantid):
    # sort by timestamp
    viewed_df = spark.read.format("iceberg").option("path", table_identifier).load()
    viewed_df = viewed_df.where(viewed_df['tenantid'] == my_tenantid)
    # get the city viewed most
    city = viewed_df.groupBy('city').count().orderBy('city').first()[0]
    viewed_df = viewed_df.where(viewed_df['city'] == city)
    # pick last 10 rows
    viewed_df = sc.parallelize(viewed_df.orderBy("timestamp").tail(10)).toDF()
    # update viewedtable
    viewed_df.write.format("iceberg").mode("overwrite").save(table_identifier)

    return viewed_df, city

def main(host, topic_name):

    # we get updated tenantid and city in the new stream process
    # Get the tenantid and city within certain timestamp period.
    my_tenantid = 'test tenantid0'
    
    # load viewedtable with tenantid and city, update with last 10 record
    viewed_df, my_city = update_viewedtable('local.db.viewedtable',my_tenantid)
    print("This is the viewedtable:")
    viewed_df.show()

    # load fulltable with city
    fulltable = spark.read.format("iceberg").option("path", 'local.db.fulltable').load()
    fulltable = fulltable.where(fulltable['city'] == my_city)
    print("This is the fulltable:")
    fulltable.show()

    # ML between local.db.viewedtable & local.db.fulltable.[city]
    print("Results:")
    result = do_recommendation.main(viewed_df, fulltable)
    # json_result is a list contains first 5 results.
    json_result = result.toJSON().take(10)
    print(json_result)
    
    # send results to kafka topic
    producer = KafkaProducer(bootstrap_servers= 'localhost:9092')
    for i in range(10):
        producer.send(topic = topic_name, value = bytes(json_result[i], 'utf-8'), key = bytes(my_tenantid, 'utf-8'))

    # consumer = KafkaConsumer(topic_name, bootstrap_servers='localhost:9092', auto_offset_reset = 'earliest', consumer_timeout_ms = float(30))
    # for msg in consumer:
    #     print(msg)

    return


if __name__ == '__main__':
    host = sys.argv[1]
    topic_name = sys.argv[2]
    conf = SparkConf()
    conf.setMaster("local").setAppName("Iceberg Test")
    conf.setAll([('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'), ('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog'), ('catalog.spark_catalog.type', 'hive'), ('spark.sql.catalog.local', 'org.apache.iceberg.spark.SparkCatalog'), ('spark.sql.catalog.local.type', 'hadoop'), ('spark.sql.catalog.local.warehouse', 'warehouse')])
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(host, topic_name)