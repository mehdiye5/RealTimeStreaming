import sys
from pyspark.ml import param
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder


from pyspark.sql import SparkSession, functions, types

# add more functions as necessary
def calculate_rating(features):
    result = features
    return result

def main(inputs, output):
    movie_ratings = session.csv("realListings.csv")
    #Create test and train set
    (training, test) = movie_ratings.randomSplit([0.8, 0.2])

    # Creating ALS model
    als = ALS(userCol="userId",  itemCol="movieId", ratingCol="rating", coldStartStrategy="drop", nonnegative= True)

    #Tune model using paramGridBuilder
    param_grid = ParamGridBuilder()\
                 .addGrid(als.rank, [12, 13, 14])\
                 .addGrid(als.maxIter, [18, 19, 20])\
                 .addGrid(als.regParam, [.17, .18, .19])\
                 .build()

    #Define evaluator as RMSE
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")

    #Build cross validation using TrainValidationSplit
    tvs = TrainValidationSplit(
        estimator=als,
        estimatorParamMaps=param_grid,
        evaluator=evaluator
    )

    #Fit ALS model to training data
    model = tvs.fit(training)

    #Extract best model from the tunning exercise using ParamGridBuilder
    best_model = model.bestModel

    #Generate predictions and evaluate using RMSE
    predictions = best_model.transform(test)
    rmse = evaluator.evaluate(predictions)

    #Print evaluation metrics and model parameters
    print("RMSE = " + str(rmse))
    print("**Best Model")
    print("  Rank:"), best_model.rank
    print(" MaxIter:"), best_model._java_obj.parent().getMaxIter()
    print(" RegParam:"), best_model._java_obj.parent().getRegParam()


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)