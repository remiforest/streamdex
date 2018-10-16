#! /usr/bin/python

import json
import sys
import os
from confluent_kafka import Producer

# Create producers dict
streams = {}

# Open source file
# load object




# Ajouter la partie DB pour le stockage des objets de base
# key = object_id

path = "/dev/index/streamdex/"
count = 1



for round in range(1000):
    with open('data.json') as f:
        data = json.load(f)
    for doc in data:
        # for each key:value pair
        # get and delete id and timestamp
        print("--------------------------------")
        print("count = {}".format(count))
        print(doc)
        object_id = doc["id"]
        del doc["id"]
        ts = doc["ts"]
        del doc["ts"]

        for key, value in doc.iteritems():
            print("processing {}:{}".format(key,value))
            print(streams)
            try:
                num_val = float(value)
                print("{} is a scalar. Continuing.".format(value))
                continue
            except Exception, e:
                print(e)
                # insert object_id into key_value stream
                print("{} is text.".format(value))
                value = value.replace("'","_")
                new_doc = {"id" : object_id, "ts" : ts}
                if not key in streams:
                    print("producer for {} does not exist yet. Creating.".format(key))
                    streams[key] = {}
                    streams[key]["topics"] = []
                    try:
                        print("creating stream {}".format(key))
                        out = os.popen("maprcli stream create -path " + path + key + " -produceperm p -consumeperm p -topicperm p").read()
                        if "(10003)" in out:
                            print("stream exists")
                            os.system("maprcli stream topic list -path " + path + key + " -json > " + key + "_topics.json")
                        # os.system("maprcli stream create -path " + path + key + " -produceperm p -consumeperm p -topicperm p")
                        streams[key]["producer"] = Producer({'streams.producer.default.stream': path + key})
                        print("producer created")
                    except Exception, e:
                        print("create stream failed")
                        print(e)
                        break
                p = streams[key]["producer"]

                print(streams[key])
                if not value in streams[key]["topics"]:
                    try:
                        print("creating topic {}:{}".format(key,value))
                        os.system("maprcli stream topic create -path " + path + key + " -topic " + value )
                        streams[key]["topics"].append(value)
                    except Exception, e:
                        print("create topic failed")
                        print(e)
          
                print("producing {}".format(json.dumps(new_doc)))
                p.produce(value, json.dumps(new_doc).encode('utf-8'))
                count += 1
                print("produced")
                # p.flush()
                # print("flushed")

