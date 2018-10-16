# streamdex

data.json is a file containing 100k randomly generated documents (id, first_name, last_name, country, vol and ts)
ingest.py loads data.json by inserting {id, ts} messages in streams:topics created on the fly for existing each key:value pair.

query.py retrieves the ids of documents filtered by a SQL like query

example : python query.py --conditions "country=France and first_name=Victor"