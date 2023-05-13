from kafka import KafkaProducer
import requests
import json
import avro.schema
import avro.io
import io
import os
from datetime import date, timedelta

def serialize(schema, obj):
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer = avro.io.DatumWriter(schema)
    writer.write(obj, encoder)
    return bytes_writer.getvalue()

def load_init_data():
    schema_file = os.getcwd()+"/dags/src/schema.avsc"
    schema = avro.schema.parse(open(schema_file).read())
    kafka_broker = 'kafka:9092'
    limit = 1000
    offset = 0

    producer = KafkaProducer(bootstrap_servers=[kafka_broker])

    api_url = 'https://publicapi.traffy.in.th/share/teamchadchart/search?limit={}&offset={}'.format(limit, offset)
    data_info = requests.get(api_url)
    set_info = json.loads(data_info.text)
    total = set_info['total']

    while offset < total:
        api_url = 'https://publicapi.traffy.in.th/share/teamchadchart/search?limit={}&offset={}'.format(limit, offset)
        data_info = requests.get(api_url)
        set_info = json.loads(data_info.text)
        for data in set_info['results']:
            serialized_data = serialize(schema, data)
            producer.send('data', value=serialized_data)
        offset += 1000
    producer.close()

<<<<<<< HEAD
load_init_data()
=======

    
def update_daily_data():
    schema_file = os.getcwd()+"/dags/src/schema.avsc"
    schema = avro.schema.parse(open(schema_file).read())
    kafka_broker = 'kafka:9092'
    limit = 1000
    offset = 0

    # Get the current date
    current_date = date.today()

    # Calculate yesterday's date
    yesterday = current_date - timedelta(days=1)

    # Format the current date as 'YYYY-MM-DD'
    start = yesterday.strftime('%Y-%m-%d')
    end = yesterday.strftime('%Y-%m-%d')

    producer = KafkaProducer(bootstrap_servers=[kafka_broker])

    api_url = 'https://publicapi.traffy.in.th/share/teamchadchart/search?limit={}&offset={}&start={}&end={}'.format(limit, offset,start,end)
    data_info = requests.get(api_url)
    set_info = json.loads(data_info.text)
    updateData = set_info['total']

    while offset < updateData:
        api_url = 'https://publicapi.traffy.in.th/share/teamchadchart/search?limit={}&offset={}&start={}&end={}'.format(limit, offset,start,end)
        data_info = requests.get(api_url)
        set_info = json.loads(data_info.text)
        for data in set_info['results']:
            serialized_data = serialize(schema, data)
            producer.send('data', value=serialized_data)
        offset += 1000
    producer.close()
>>>>>>> 004337fb2f122ecda648c13f6d46a54baac49ad4
