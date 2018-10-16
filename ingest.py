#! /usr/bin/python

import json
import sys
import os
import requests
import subprocess

from requests.auth import HTTPBasicAuth
from confluent_kafka import Producer

# Create producers dict
streams = {}

# Open source file
# load object

def create_stream(path,name):
    escaped_path = path.replace('/','%2F')
    url = 'https://127.0.0.1:8443/rest/stream/create?path=' + escaped_path + name + "&produceperm=p&consumeperm=p&topicperm=p"
    # print("url = {}".format(url))
    req = requests.post(url,verify=False,auth=HTTPBasicAuth('mapr', 'mapr'))
    return req.text    
    # return os.popen("maprcli stream create -path " + path + name + " -produceperm p -consumeperm p -topicperm p").read()    

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

for round in range(2):
    with open('data.json') as f:
        data = json.load(f)
    for doc in data:
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
            except Exception as e:
                # print(e)
                # if value is not a number
                # insert object_id into key_value stream
                # print("{} is text.".format(value))
                value = format_value(value)
                new_doc = {"id" : object_id, "ts" : ts} # doc to be insterted in the stream
                if not key in streams: # if the stream isn't referenced it's created
                    # print("unknown stream, testing")
                    streams[key] = {}
                    # streams[key]["topics"] = []
                    try:
                        # testing if stream exists
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
          
                print("producing {} into {}:{}".format(json.dumps(new_doc),path+key,value))
                p.produce(value, json.dumps(new_doc).encode('utf-8'))
                count += 1
                # print("produced")


