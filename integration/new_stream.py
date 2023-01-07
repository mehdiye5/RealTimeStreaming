from kafka import KafkaProducer
from kafka import KafkaConsumer
import json
import sys

def main():


    #df['city'], df['zpid'], df['garageSpaces'], df['hasHeating'], df['latitude'] ,df['longitude'], df['hasView'], df['homeType'], df['parkingSpaces'],df['yearBuilt'], df['latestPrice'], df['lotSizeSqFt'], df['livingAreaSqFt'], df['numOfPrimarySchools'], df['numOfElementarySchools'], df['numOfMiddleSchools'],df['numOfHighSchools'], df['avgSchoolDistance'], df['avgSchoolRating'], df['numOfBathrooms'] ,df['numOfBedrooms'], df['numOfStories']

    # |austin|121719682|           2|      true|30.490257263183604|-97.79174041748048|  false|  Residential|            0|     2015|   699000.0|    10890.0|        4020.0|                  1|                     0|                 1|               1| 3.266666666666667| 7.666666666666668|           4.0|          4.0|           2|
    tenantid = "test tenantid"
    message1 = '''{"city": "austin", "zpid": "121719682", "garageSpaces": 2, "hasHeating": true, "latitude": 30.490257263183604, "longitude": -97.79174041748048, "hasView": false, "homeType": "Residential", "parkingSpaces" : 0, "yearBuilt": 2015, "latestPrice": 699000.0, "lotSizeSqFt": 10890.0, "livingAreaSqFt": 4020.0, "numOfPrimarySchools": 1, "numOfElementarySchools": 0, "numOfMiddleSchools": 1, "numOfHighSchools": 1, "avgSchoolDistance": 3.266666666666667, "avgSchoolRating": 7.666666666666668, "numOfBathrooms": 4.0, "numOfBedrooms": 4.0, "numOfStories": 2}'''

    producer = KafkaProducer(bootstrap_servers= 'localhost:9092')
    for _id in range(10):
        temp_tenantid = tenantid + str(_id)
        producer.send(topic = 'viewed_stream', value = bytes(message1, 'utf-8'), key = bytes(temp_tenantid, 'utf-8'))


    consumer = KafkaConsumer('viewed_stream', bootstrap_servers='localhost:9092', auto_offset_reset = 'earliest', consumer_timeout_ms = float(30))

    for msg in consumer:
        print(msg)


if __name__ == '__main__':

    main()