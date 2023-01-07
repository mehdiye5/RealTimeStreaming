from kafka import KafkaProducer
from kafka import KafkaConsumer
import json

f = open('sample.json')
message = str(json.load(f))
producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send(topic = 'python_topic', value = bytes(message, 'utf-8'), key = b'test key')

consumer = KafkaConsumer('python_topic', bootstrap_servers='localhost:9092', auto_offset_reset = 'earliest', consumer_timeout_ms = float(30))

for msg in consumer:
    print(msg)
