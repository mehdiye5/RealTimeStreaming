import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
#import pandas as pd
from pyspark.sql import SparkSession, functions, session, types, Row, pandas
from math import radians, cos, sin, asin, sqrt, degrees, atan2
from pyspark.sql.functions import col, pandas_udf
#import numpy as np


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



def main(inputs, output):
    # main logic starts here
    """
    listing_schema = types.StructType([
    types.StructField('lang', types.StringType())    
    ])
    """

    df = spark.read.csv(inputs, header=True, inferSchema=True)
    df = df.cache()
    #df.printSchema()
    #listings = df.select(df['zpid'], df['city'],df['streetAddress'], df['zipcode'], df['latitude'] ,df['longitude'])
    listings = df
    target = df.where(df['zpid'] == "120900430")
    target = target.select(target['zpid'].alias('target_zpid'), target['latitude'].alias('target_latitude'), target['longitude'].alias('target_longitude'))
    

    distance = functions.udf(calculate_distance, returnType=types.DoubleType())
    result = listings.join(target)
    result = result.na.drop(subset=["longitude","latitude"])
    
    #result = result.withColumn("longitude",result['longitude'].cast(types.DoubleType()).alias('longitude'))
    #result = result.withColumn("latitude",result['latitude'].cast(types.DoubleType()).alias('latitude'))
    #result = result.withColumn("target_longitude",result['target_longitude'].cast(types.DoubleType()).alias('target_longitude'))
    #result = result.withColumn("target_latitude",result['target_latitude'].cast(types.DoubleType()).alias('target_latitude'))
    #result = result.select(result, (functions.radians(functions.col('longitude')) - functions.radians(functions.col('target_longitude'))).alias('dlon'))
    
    result = result.select(result, (functions.radians(result['longitude']) - functions.radians(result['target_longitude'])).alias('dlon'))
    result = result.select(result, (functions.radians(result['latitude']) - functions.radians(result['target_latitude'])).alias('dlat'))
    result = result.select(result, (functions.sin(result['dlat']/2)**2 + functions.cos(result['target_latitude']) * functions.cos(result['latitude']) * functions.sin(result['dlon'] / 2)**2 ).alias('a'))
    result = result.select(result, (functions.atan2(functions.sqrt(result['a']), functions.sqrt(1 - result['a']))).alias('distance'))
    result = result.sort(result['distance'])


    
    
    result.show(5)
    #target.show(10)
    #rdd = df.rdd
    #rdd = rdd.map(lambda r: map_listings(r, t))
    #a = rdd.collect()

    #for i in a[0:1]:
    #    print(i)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)