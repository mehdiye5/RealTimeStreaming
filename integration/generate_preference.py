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




def test_model(model_file):

    # suppose we have a real estate list 
    test_city = 'driftwood'
    fulltable = spark.read.format("iceberg").option("path", 'local.db.fulltable').load()
    testtable = fulltable.where(fulltable['city'] == test_city)

    # load the model
    model = PipelineModel.load(model_file)

    # use the model to make predictions

    # we could use the model for tenant to tell the tenant's perference for real estate.
    predictions = model.transform(testtable)
    predictions.show(10)
    
    # evaluate the predictions
    # r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='score',
    #         metricName='r2')
    # r2 = r2_evaluator.evaluate(predictions)

    # print("R2 values is: ", r2)
    
    # rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='score',
    #         metricName='rmse')
    # rmse = rmse_evaluator.evaluate(predictions)

    # print("The RMSE is:", rmse)

    # print(model.stages[-1].featureImportances)


if __name__ == '__main__':
    model_file = sys.argv[1]
    conf = SparkConf()
    conf.setMaster("local").setAppName("Iceberg Test")
    conf.setAll([('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'), ('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog'), ('catalog.spark_catalog.type', 'hive'), ('spark.sql.catalog.local', 'org.apache.iceberg.spark.SparkCatalog'), ('spark.sql.catalog.local.type', 'hadoop'), ('spark.sql.catalog.local.warehouse', 'warehouse')])
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    test_model(model_file)
