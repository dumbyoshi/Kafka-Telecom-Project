#from ksql import KSQLAPI
# client = KSQLAPI('http://localhost:8088')

# client.ksql('show tables')
import requests
import json

from kafka import KafkaConsumer
consumer = KafkaConsumer('CALL_DATASETS',auto_offset_reset ='earliest')
for message in consumer:
    #print ((message.key).decode("utf-8") )
    call_data = json.loads((message.value).decode('utf-8')) # decode converts byte to string, json.loads converts stringified dictionary to dictionary

    #print(json.loads((message.value).decode("utf-8")))
    if (call_data['CALL_DIRECTION'] == 'Incoming'):
        URL = "<webhook url>" #slack webhook url
        body = {"text":"Hello ! It's an incoming call "}
        headers = {"Content-Type":"application/json"}

        result = requests.post(URL, data = json.dumps(body),headers=headers) #json.dumps converts python object to json object

        print(result)
