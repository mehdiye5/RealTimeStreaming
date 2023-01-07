import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
#import pandas as pd
from pyspark.sql import SparkSession, functions, session, types, Row, pandas
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from math import radians, cos, sin, asin, sqrt, degrees, atan2
from pyspark.sql.functions import col, pandas_udf
#import numpy as np

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.regression import GBTRegressor

from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer


def main():
    schema = types.StructType([
        types.StructField('zpid', types.StringType()),
        types.StructField('city', types.StringType()),
    ])
    df = spark.createDataFrame([('123', 'austin'), ('234','sda'), ('2341','austin')], schema)
    city = df.groupBy('city').count().orderBy('city').first()[0]
    df = df.where(df['city'] == city)
    df.show()



if __name__ == '__main__':
    conf = SparkConf()
    conf.setMaster("local").setAppName("Iceberg Test")
    conf.setAll([('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'), ('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog'), ('catalog.spark_catalog.type', 'hive'), ('spark.sql.catalog.local', 'org.apache.iceberg.spark.SparkCatalog'), ('spark.sql.catalog.local.type', 'hadoop'), ('spark.sql.catalog.local.warehouse', 'warehouse')])
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()
