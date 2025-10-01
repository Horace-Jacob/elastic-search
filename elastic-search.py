from elasticsearch import Elasticsearch
import argparse

"""
this is elastic search example
"""
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
    cloud_id=ES_CLOUD_ID,
    basic_auth=ES_BASIC_AUTH
)

# basic search with match all
search_query = {
    "match_all": {
    }
}

result = es.search(index=ES_INDEX_NAME, query=search_query)

for hit in result['hits']['hits']:
    print(hit['_source'])
