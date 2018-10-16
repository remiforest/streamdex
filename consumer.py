#! /usr/bin/python3

import argparse
import json
import os
from confluent_kafka import Consumer, KafkaError
import time

parser = argparse.ArgumentParser(description='Launch a stream consumer and display messages')
parser.add_argument('--stream',help='path to the stream',required=True)
parser.add_argument('--topic',help='topic name',default="default_topic")
parser.add_argument('--group',help='group name',default="default_group")
args = parser.parse_args()

c = Consumer({'group.id': args.group,'default.topic.config': {'auto.offset.reset': 'earliest'}})
c.subscribe([args.stream+":"+args.topic])


print("Subscribing to stream {} with group {}".format(args.stream+":"+args.topic,args.group))


running = True
nb_received = 0
while running:
  msg = c.poll(timeout=1.0)
  if msg is None:
    continue
  if not msg.error():
    print(msg.value())
    nb_received += 1
    if nb_received % 10 == 0:
        print("{} messages displayed".format(nb_received))
  elif msg.error().code() != KafkaError._PARTITION_EOF:
    print(msg.error())

c.close()

