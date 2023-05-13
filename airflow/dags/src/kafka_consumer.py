# import required libraries
from kafka import KafkaConsumer
import avro.schema
import avro.io
import io
import os
import pandas as pd
import numpy as np

def get_data():
    df = pd.DataFrame()
    schema_file = os.getcwd()+"/dags/src/schema.avsc"
    schema = avro.schema.parse(open(schema_file).read())

    def deserialize(schema, raw_bytes):
        bytes_reader = io.BytesIO(raw_bytes)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        return reader.read(decoder)

    # Connect to kafka broker running in your local host (docker). Change this to your kafka broker if needed
    kafka_broker = 'localhost:9092'

    consumer = KafkaConsumer(
        'avro',
        bootstrap_servers=[kafka_broker],
        enable_auto_commit=True,
        value_deserializer=lambda x: deserialize(schema, x))

    print('Running Consumer with AVRO')
    for message in consumer:
        print(message.value)
        # df = df.append(message.value, ignore_index= True)
        df = df._append(pd.Series(message.value,index=df.columns,name=len(df)))
    
    # df = pd.DataFrame({'name':['Jeff','Rob'],'age':[21,22]})
    # df = df._append(pd.Series(['Nenenii',18],index=df.columns,name=len(df)))
    # print(df)
    df.to_csv('./data/df.csv')  
    consumer.close()

get_data()