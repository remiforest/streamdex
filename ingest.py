#! /usr/bin/python

import json
import sys
import os
import requests
import time
import subprocess

from requests.auth import HTTPBasicAuth
from confluent_kafka import Producer
from mapr.ojai.storage.ConnectionFactory import ConnectionFactory

# Create producers dict
streams = {}

def create_stream(path,name):
    escaped_path = path.replace('/','%2F')
    url = 'https://127.0.0.1:8443/rest/stream/create?path=' + escaped_path + name + "&produceperm=p&consumeperm=p&topicperm=p"
    req = requests.post(url,verify=False,auth=HTTPBasicAuth('mapr', 'mapr'))
    return req.text    

def format_value(value):
    value = value.replace("'","_") # Handling names with '
    value = value.replace(" ","_") # Handling names with spaces
    value = value.replace(",","_") # Handling names with ,
    value = value.replace(";","_") # Handling names with ;
    value = value.replace("@","_") # Handling names with ,
    return value

path = "/dev/index/streamdex/"
fullpath = "/mapr/demo.mapr.com/dev/index/streamdex/"
count = 1

table_name = "transactions"
table_path = path + table_name

# Create a connection to data access server
connection_str = "localhost:5678?auth=basic;user=mapr;password=mapr;ssl=false"
connection = ConnectionFactory.get_connection(connection_str=connection_str)

# Get a store and assign it as a DocumentStore object
if connection.is_store_exists(table_path):
    store = connection.get_store(table_path)
else:
    store = connection.create_store(table_path)

start_time = time.time()


with open('data.json') as f:
    data = json.load(f)
for doc in data:
    append = True
    start = time.time()
    print("--------------------------------")
    print("count = {}".format(count))
    print(doc)
    object_id = doc["id"]
    del doc["id"]
    ts = doc["ts"]
    del doc["ts"]

    for key, value in doc.iteritems():
        # print("processing {}:{}".format(key,value))
        try:
            # test is value is a number
            num_val = float(value)
            # print("{} is a number. Skipping.".format(value))
            continue
        except :
            value = format_value(value)
            new_doc = {"_id" : object_id, "ts" : ts} # doc to be insterted in the stream
            if not key in streams: # if the stream isn't referenced it's created
                streams[key] = {}
                try:
                    if os.path.islink(fullpath + key): # stream exists
                        print("stream exists")
                    else:
                        print("creating stream {}".format(key))
                        out = create_stream(path,key)
                        print(out)
                    streams[key]["producer"] = Producer({'streams.producer.default.stream': path + key})
                except Exception as e:
                    print("create stream failed")
                    print(e)
                    break
            p = streams[key]["producer"]      
            try:
                 p.produce(value, json.dumps(new_doc).encode('utf-8'))
            except:
                print("produce failed")
                append = False
                break
    doc["_id"] = str(object_id)
    doc["ts"] = ts
    count += 1
    if append:
        store.insert_or_replace(connection.new_document(dictionary=doc))
        process_time = round((time.time() - start)*1000,0)
        total_process_time = time.time() - start_time
        average_process_time = round(total_process_time *1000 / count,0)
        print("inserted - process time : {} ms - average = {} ms".format(process_time,average_process_time))