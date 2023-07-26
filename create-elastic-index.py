from elasticsearch import Elasticsearch
import psycopg2
import argparse


def parse_args():
    parser = argparse.ArgumentParser(
        description="Listen for PostgreSQL notifications and sync data to Elasticsearch.")
    parser.add_argument("--es_cloud_id", type=str, required=False,
                        help="Elasticsearch Cloud ID for authentication.")
    parser.add_argument("--es_username", type=str, required=False,
                        help="Elasticsearch Username for authentication")
    parser.add_argument("--es_password", type=str, required=False,
                        help="Elasticsearch password for authentication")
    parser.add_argument("--es_index", type=str, required=False,
                        help="Elasticsearch Index Name")
    return parser.parse_args()


args = parse_args()

ES_CLOUD_ID = "YOUR CLOUD ID" or args.es_cloud_id
ES_BASIC_AUTH = ("YOUR ELASTICSEARCH USERNAME" or args.es_username,
                 "YOUR ELASTICSEARCH PASSWORD" or args.es_password)

ES_INDEX_NAME = "YOUR ELASTICSEARCH INDEX NAME" or args.es_index


es = Elasticsearch(
    cloud_id="My_deployment:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvJDk2NTU5N2I2YmY4YTRmZThiZmMwMDgzZTc2ZTFlZjhlJDZiZTg4NGRhZDI2NzRkNWU4NGQ4Y2JlMjA4OTBmNThi",
    basic_auth=("elastic", "frnh2u9kvswtX7nDfUel6iAI")
)

DB_NAME = "YOUR DATABASE NAME"
DB_USER = "YOUR DATABASE USERNAME"
DB_PASS = "YOUR DATABASE PASSWORD"
DB_PORT = "YOUR DATABASE PORT"

connection = psycopg2.connect(
    database=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT)

cursor = connection.cursor()

cursor.execute("select * from Test")

rows = cursor.fetchall()

for data in rows:
    doc = {
        'id': data[0],
        'about': data[1]
    }

    res = es.index(index=ES_INDEX_NAME, document=doc)

    if res['result'] == 'created':
        print(f"Document {data[0]} indexed successfully")
    else:
        print(f"Failed to index document {data[0]}")

print("Data indexed successfully")
connection.close()
