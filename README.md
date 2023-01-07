# RealTimeStreaming

# Real-time-streaming-real-estate-listings

Setting up PoC locally
========================
Kafka set up:
- brew install kafka
- Run these two commands
    - brew services start zookeeper
    - brew services start kafka

To stop:
    - brew services stop zookeeper
    - brew services stop kafka

Iceberg set up:
- download iceberg jar file from https://iceberg.apache.org/#getting-started/
- run command with `spark-shell --packages org.apache.iceberg:iceberg-spark3-runtime:0.12.0`
- Set up db table test:
```
spark-sql --packages org.apache.iceberg:iceberg-spark3-runtime:0.12.0\
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hive \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=warehouse
```
`CREATE TABLE local.db.test (zpid string, city string, streetAddress string, zipcode string) USING iceberg;`

`${SPARK_HOME}/bin/spark-submit --jars iceberg-spark3-runtime-0.12.0.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 nrt_real_estate.py localhost:9092 python_topic local.db.test`


Integration steps:

Create two table:
- create fulltable
    create inside the load_fulltable.py file
- create viewedtable
    CREATE TABLE local.db.viewedtable (tenantid string, city string, zpid string, garageSpaces integer, hasHeating boolean, latitude double, longitude double, hasView boolean, homeType string, parkingSpaces integer, yearBuilt integer, latestPrice double, lotSizeSqFt double, livingAreaSqFt double, numOfPrimarySchools integer, numOfElementarySchools integer, numOfMiddleSchools integer, numOfHighSchools integer, avgSchoolDistance double, avgSchoolRating double, numOfBathrooms double, numOfBedrooms double, numOfStories integer, timestamp timestamp) USING iceberg partitioned by (tenantid);

Load the fulltable partitioned by city:
- ${SPARK_HOME}/bin/spark-submit --jars iceberg-spark3-runtime-0.12.0.jar load_fulltable.py austinHousingData.csv local.db.fulltable

<!-- viewed_stream and full_stream is the name of two kafka topic contain rts -->
Streaming:
- open two terminal
- update viewedtable:
    ${SPARK_HOME}/bin/spark-submit --jars iceberg-spark3-runtime-0.12.0.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 updateviewedtable.py localhost:9092 viewed_stream local.db.viewedtable
- update fulltable:
    ${SPARK_HOME}/bin/spark-submit --jars iceberg-spark3-runtime-0.12.0.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 updatefulltable.py localhost:9092 full_stream local.db.fulltable

Integration:
- make sure kafka service runing
<!-- list here is a list of tenantid -->
- get model:
    ${SPARK_HOME}/bin/spark-submit --jars iceberg-spark3-runtime-0.12.0.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 get_model.py list
<!-- result_topic here is the kafka topic stores the recommendation -->
- return recommendation:
    ${SPARK_HOME}/bin/spark-submit --jars iceberg-spark3-runtime-0.12.0.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 return_result.py localhost:9092 result_topic
<!-- given a tenant model and a list of real estate, we could predict the real estate the tenant prefer -->
- Usage of model:
    ${SPARK_HOME}/bin/spark-submit --jars iceberg-spark3-runtime-0.12.0.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 generate_preference.py my_model



========
Back end server
    - Set up DB Locally
        - brew install mysql
        - mysql -uroot
        - create database nrt_real_estate
        - create user 'user'@'localhost' identified by 'ThePassword';
        - grant all on nrt_real_estate.* to 'user'@'localhost';
        - USE nrt_real_estate    
        (Now you can use sql queries through CLI)
    - brew services start zookeeper
    - brew services start kafka
    - brew services start mysql
--topic recommendationListing

    - Install Intellij and import the project by open project -> Click on Pom.xml
    - Press Run or Debug to start up the service 
        - Starting up the service will create the required DB & Kafka topics
            - (For Info only)
                - (Not needed if you start service) kafka-topics  --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic house
                - (Not needed if you start service) kafka-topics  --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic listing
                - (Not needed if you start service) kafka-topics  --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 
    
    - Use Postman To make HTTP calls

    - Kafka consumer/producer setup
        - kafka-console-consumer --bootstrap-server localhost:9092 --topic house --from-beginning
        - kafka-console-consumer --bootstrap-server localhost:9092 --topic listing --from-beginning
        - kafka-console-producer --broker-list localhost:9092 --topic recommendationListing --property "parse.key=true" --property "key.separator=:"

        Example payload for producer:
        tenantId:["zpid, zpid2, zpid3, zpid4"]



Shutdown
    - brew services stop zookeeper
    - brew services stop kafka
    - brew services stop mysql
    - kafka-topics  --delete --bootstrap-server localhost:9092 --topic recommendationListing etc...




