#! /usr/bin/python

import json
import sys
import os
import subprocess

from confluent_kafka import Producer

# Create producers dict
streams = {}

# Open source file
# load object

def create_stream(path,name):
    return os.popen("maprcli stream create -path " + path + name + " -produceperm p -consumeperm p -topicperm p").read()    

def create_topic(stream,name):
    return os.popen("maprcli stream topic create -path " + stream + " -topic " + name).read()    

def get_topics(path,name):
    child = subprocess.Popen("maprcli stream topic list -path " + path + name + " -json",shell=True,stdout=subprocess.PIPE)
    output = child.communicate()[0]
    out_doc = json.loads(output)
    topics = []
    for topic in out_doc["data"]:
        topics.append(topic["topic"])
    return(topics)


# Ajouter la partie DB pour le stockage des objets de base
# key = object_id

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
            print("processing {}:{}".format(key,value))
            try:
                # test is value is a number
                num_val = float(value)
                print("{} is a number. Skipping.".format(value))
                continue
            except Exception, e:
                print(e)
                # if value is not a number
                # insert object_id into key_value stream
                print("{} is text.".format(value))
                value = value.replace("'","_") # Handling names with ' 
                new_doc = {"id" : object_id, "ts" : ts} # doc to be insterted in the stream
                if not key in streams: # if the stream isn't referenced it's created
                    print("unknown stream, testing")
                    streams[key] = {}
                    streams[key]["topics"] = []
                    try:
                        # testing if stream exists
                        if os.path.islink(fullpath + key): # stream exists
                            print("stream exists")
                            topics = get_topics(path,key)
                            print("existing topics for stream {}".format(path + key))
                            print(topics)
                            streams[key]["topics"] = topics
                        else:
                            print("creating stream {}".format(key))
                            out = create_stream(path,key)
                        streams[key]["producer"] = Producer({'streams.producer.default.stream': path + key})
                    except Exception, e:
                        print("create stream failed")
                        print(e)
                        break
                p = streams[key]["producer"]

                if not value in streams[key]["topics"]: # if topic doesn't exists it's created
                    try:
                        print("creating topic {}:{}".format(key,value))
                        out = create_topic(path + key, value)
                        streams[key]["topics"].append(value)
                    except Exception, e:
                        print("create topic failed")
                        print(e)
          
                print("producing {}".format(json.dumps(new_doc)))
                p.produce(value, json.dumps(new_doc).encode('utf-8'))
                count += 1
                print("produced")


