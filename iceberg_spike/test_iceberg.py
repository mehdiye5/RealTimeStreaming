import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

# add more functions as necessary

def main(inputs, output):
    # main logic starts here
    db = spark.sql("select * from local.db.table")
    print(db.show())
    return


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    conf = SparkConf()
    conf.setMaster("local").setAppName("Iceberg Test")
    conf.setAll([('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'), ('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog'), ('catalog.spark_catalog.type', 'hive'), ('spark.sql.catalog.local', 'org.apache.iceberg.spark.SparkCatalog'), ('spark.sql.catalog.local.type', 'hadoop'), ('spark.sql.catalog.local.warehouse', 'warehouse')])
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)