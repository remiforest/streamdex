#! /usr/bin/python

import requests
from requests.auth import HTTPBasicAuth

req = requests.get('https://127.0.0.1:8443/rest/stream/topic/list?path=%2Fdev%2Findex%2Fstreamdex%2Fcountry',verify=False,auth=HTTPBasicAuth('mapr', 'mapr'))
print(req.text)