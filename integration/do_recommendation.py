import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
#import pandas as pd
from pyspark.sql import SparkSession, functions, session, types, Row, pandas
from math import radians, cos, sin, asin, sqrt, degrees, atan2
from pyspark.sql.functions import col, pandas_udf
#import numpy as np

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.regression import GBTRegressor

from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer


def main(target, fulltable):
    # main logic starts here
    """
    listing_schema = types.StructType([
    types.StructField('lang', types.StringType())    
    ])
    """


    # build listing and target dataframes
    df = fulltable
    
    listings = df.select(df['zpid'], df['garageSpaces'], df['hasHeating'], df['latitude'] ,df['longitude'], df['hasView'], df['homeType'], df['parkingSpaces'],
                        df['yearBuilt'], df['latestPrice'], df['lotSizeSqFt'], df['livingAreaSqFt'], df['numOfPrimarySchools'], df['numOfElementarySchools'], df['numOfMiddleSchools'],
                        df['numOfHighSchools'], df['avgSchoolDistance'], df['avgSchoolRating'], df['numOfBathrooms'] ,df['numOfBedrooms'], df['numOfStories'])
    
    

    # Feature Engineering
    target = target.select(target['zpid'].alias('target_zpid'), target['latitude'].alias('target_latitude'), 
                           target['longitude'].alias('target_longitude'), target['garageSpaces'].alias('target_garageSpaces'), 
                           target['hasHeating'].alias('target_hasHeating'), target['hasView'].alias('target_hasView'),
                           target['homeType'].alias('target_homeType'), target['parkingSpaces'].alias('target_parkingSpaces'),
                           target['yearBuilt'].alias('target_yearBuilt'), target['lotSizeSqFt'].alias('target_lotSizeSqFt'),
                           target['livingAreaSqFt'].alias('target_livingAreaSqFt'), target['numOfPrimarySchools'].alias('target_numOfPrimarySchools'),
                           target['numOfElementarySchools'].alias('target_numOfElementarySchools'), target['numOfMiddleSchools'].alias('target_numOfMiddleSchools'),
                           target['numOfHighSchools'].alias('target_numOfHighSchools'), target['avgSchoolDistance'].alias('target_avgSchoolDistance'),
                           target['avgSchoolRating'].alias('target_avgSchoolRating'), target['numOfBathrooms'].alias('target_numOfBathrooms'),
                           target['numOfBedrooms'].alias('target_numOfBedrooms'), target['numOfStories'].alias('target_numOfStories'),
                           target['latestPrice'].alias('target_latestPrice'))
    

    
    result = listings.join(target)
    result = result.na.drop(subset=["longitude","latitude"])

    result = result.withColumn("score", functions.lit(0))
    
    result = result.withColumn("longitude",result['longitude'].cast(types.DoubleType()).alias('longitude'))
    result = result.withColumn("latitude",result['latitude'].cast(types.DoubleType()).alias('latitude'))
    result = result.withColumn("target_longitude",result['target_longitude'].cast(types.DoubleType()).alias('target_longitude'))
    result = result.withColumn("target_latitude",result['target_latitude'].cast(types.DoubleType()).alias('target_latitude'))
    
    
    result = result.select(result["*"], (functions.radians(result['longitude']) - functions.radians(result['target_longitude'])).alias('dlon'))
    result = result.select(result["*"], (functions.radians(result['latitude']) - functions.radians(result['target_latitude'])).alias('dlat'))
    result = result.select(result["*"], (functions.sin(result['dlat']/2)**2 + functions.cos(result['target_latitude']) * functions.cos(result['latitude']) * functions.sin(result['dlon'] / 2)**2 ).alias('temp_a'))
    result = result.select(result["*"], functions.abs(result['temp_a']).alias('a'))
    # Calculate the distance based on the logitude an lattitude
    result = result.select(result["*"], (functions.atan2(functions.sqrt(result['a']), functions.sqrt(1 - result['a']))).alias('distance'))
    #Drop unwanted columns
    result = result.drop('dlon', 'dlat', 'temp_a', 'a', 'target_latitude', 'target_longitude')
    result = result.where(result['distance'] != 0)
    result = result.where(result['homeType'] == result['target_homeType'])    
    
    # Since distance has a very wide range we have to log scale it
    result = result.withColumn("distance", functions.abs(functions.log(result['distance'])))
    

    # calculating the score
    result = result.withColumn("score", result['score'] + (1/result['distance']) * 10)
    result = result.withColumn("score", result['score'] + functions.when(result['garageSpaces'] >= result['target_garageSpaces'], 1).otherwise(0))
    result = result.withColumn("score", result['score'] + functions.when(result['garageSpaces'] >= result['target_garageSpaces'], 1).otherwise(0))
    result = result.withColumn("score", result['score'] + functions.when(result['hasHeating'] == result['target_hasHeating'], 1).otherwise(0))
    result = result.withColumn("score", result['score'] + functions.when(result['hasView'] == result['target_hasView'], 0.5).otherwise(0))
    result = result.withColumn("score", result['score'] + functions.when(result['parkingSpaces'] >= result['target_parkingSpaces'], 1).otherwise(0))
    result = result.withColumn("score", result['score'] + functions.when(result['yearBuilt'] >= result['target_yearBuilt'], 0.7).otherwise(0))
    result = result.withColumn("score", result['score'] + functions.when(result['lotSizeSqFt'] >= result['target_lotSizeSqFt'], 1.5).otherwise(0))
    result = result.withColumn("score", result['score'] + functions.when(result['livingAreaSqFt'] >= result['target_livingAreaSqFt'], 1.5).otherwise(0))
    result = result.withColumn("score", result['score'] + functions.when(result['numOfPrimarySchools'] >= result['target_numOfPrimarySchools'], 0.4).otherwise(0))
    result = result.withColumn("score", result['score'] + functions.when(result['numOfElementarySchools'] >= result['target_numOfElementarySchools'], 0.4).otherwise(0))
    result = result.withColumn("score", result['score'] + functions.when(result['numOfMiddleSchools'] >= result['target_numOfMiddleSchools'], 0.4).otherwise(0))
    result = result.withColumn("score", result['score'] + functions.when(result['numOfHighSchools'] >= result['target_numOfHighSchools'], 0.4).otherwise(0))
    result = result.withColumn("score", result['score'] + functions.when(result['avgSchoolDistance'] <= result['target_avgSchoolDistance'], 1).otherwise(0))
    result = result.withColumn("score", result['score'] + functions.when(result['avgSchoolRating'] >= result['target_avgSchoolRating'], 1).otherwise(0))    
    result = result.withColumn("score", result['score'] + functions.when(result['numOfBathrooms'] >= result['target_numOfBathrooms'], 1.5).otherwise(0))
    result = result.withColumn("score", result['score'] + functions.when(result['numOfBedrooms'] >= result['target_numOfBedrooms'], 1.5).otherwise(0))
    result = result.withColumn("score", result['score'] + functions.when(result['numOfStories'] == result['target_numOfStories'], 1).otherwise(0))
    result = result.withColumn("score", result['score'] + (result['target_latestPrice']/result['latestPrice'])*4)

    # Cast the boolean to integer
    result = result.withColumn('hasHeating', result['hasHeating'].cast("integer"))
    result = result.withColumn('hasView', result['hasView'].cast("integer"))
    
    result = result.sort(result['score'].desc())
    result = result.dropDuplicates(['zpid'])
    

    # drop unwanted columns
    result = result.drop('target_zpid', 'target_garageSpaces', 'target_hasHeating', 'target_hasView', 'target_homeType',
                        'target_parkingSpaces', 'target_yearBuilt', 'target_lotSizeSqFt', 'target_livingAreaSqFt', 
                        'target_numOfPrimarySchools', 'target_numOfElementarySchools', 'target_numOfMiddleSchools',
                        'target_numOfHighSchools', 'target_avgSchoolDistance', 'target_avgSchoolRating', 'target_numOfBedrooms',
                        'target_numOfStories', 'target_latestPrice', 'target_numOfBathrooms', 'homeType', 'distance')

    #result.show(5)
    
    #df = df.drop('description', 'homeImage')
    
    #df.repartition(1).write.option("header",True).option("delimiter",".").format("csv").save("./savedData")

   
    
    result = result.select('zpid')
    return result
    
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    model_name = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(target, fulltable)