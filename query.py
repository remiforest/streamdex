#! /usr/bin/python3

import argparse
import json
import os
import time
import uuid

from confluent_kafka import Consumer, KafkaError
from mapr.ojai.storage.ConnectionFactory import ConnectionFactory

parser = argparse.ArgumentParser(description='Query streams')
parser.add_argument('--conditions',help='SQL like query conditions. ie : "cond1=a and cond2=b"',required=True)
args = parser.parse_args()

path = "/dev/index/streamdex/"
table_name = "transactions"
table_path = path + table_name

# Create a connection to data access server
connection_str = "localhost:5678?auth=basic;user=mapr;password=mapr;ssl=false"
connection = ConnectionFactory.get_connection(connection_str=connection_str)

store = connection.get_store(table_path)


conditions = args.conditions.split(" and ")

# conditions format : condition1=value1 and condition2=value2
ids = {}
numeric_filters = {}

for condition in conditions:
    if not "=" in condition:
        numeric_filters.append(condition)
        continue  
    print(condition)
    ids[condition]=[]
    c = Consumer({'group.id': str(uuid.uuid4()),'default.topic.config': {'auto.offset.reset': 'earliest'}})
    st = condition.split("=")
    stream = st[0]
    topic = st[1]
    print("subscribing to {}".format(path + stream + ":" + topic))
    c.subscribe([path + stream + ":" + topic])

    nb_received = 0
    while True:
      msg = c.poll(timeout=1.0)
      # print(msg.value())
      if msg is None:
        break
      if not msg.error():
        doc = json.loads(msg.value())
        ids[condition].append(doc["_id"])
        # print(ids)
        nb_received += 1
      elif msg.error().code() == KafkaError._PARTITION_EOF:
        break
      else:
        print(msg.error())
    
    print("\n\n {} ids retreived".format(nb_received))
    c.close()

# intersect indexes

# select smallest array
min_len = float("inf")
for k,v in ids.iteritems():
    if len(v) < min_len:
        min_len = len(v)
        min_idx = k

print(min_idx)
print(min_len)

result = []

for _id in ids[min_idx]:
    for k,v in ids.iteritems():
        if k != min_idx:
            if not _id in v:
                break
        result.append(str(_id))

print(result)

query = {"$where":{"$in": {"_id": result}}}
query_result = store.find(query)

for doc in query_result:
    print(doc)
