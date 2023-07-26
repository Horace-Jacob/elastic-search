import asyncio
import asyncpg_listen
from elasticsearch import Elasticsearch
import json
import argparse

DB_NAME = "YOUR DATABASE NAME"
DB_USER = "YOUR DATABASE USERNAME"
DB_PASS = "YOUR DATABASE PASSWORD"
DB_PORT = "YOUR DATABASE PORT"


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

# establish connection to elasticsearch
es = Elasticsearch(cloud_id=ES_CLOUD_ID, basic_auth=ES_BASIC_AUTH)


async def handle_notifications(notification: asyncpg_listen.NotificationOrTimeout) -> None:
    try:
        payload = notification.payload
        # Process the received notification payload and update Elasticsearch accordingly
        # Assuming that the payload is in JSON format
        action_data = json.loads(payload)
        action = action_data["action"]
        data = action_data["data"]

        if action == "INSERT" or action == "UPDATE":
            # Index the data in Elasticsearch for INSERT or UPDATE actions
            index_to_elasticsearch(data)
            print("Data has been synced to elastic")
        elif action == "DELETE":
            # Delete the data from Elasticsearch for DELETE actions
            delete_from_elasticsearch(data)
            print("Data has been deleted from elastic")
    except asyncio.TimeoutError:
        print("Notification timeout occurred. No notifications received.")
    except Exception as e:
        print(f"An error occurred while processing the notification: {e}")


def index_to_elasticsearch(data):
    doc = {
        "id": data["id"],
        "about": data["about"]
    }

    es.index(index=ES_INDEX_NAME, id=doc["id"], document=doc)


def delete_from_elasticsearch(data):
    es.delete(index=ES_INDEX_NAME, id=data["id"])


async def main():
    # Create the PostgreSQL notification listener
    listener = asyncpg_listen.NotificationListener(
        asyncpg_listen.connect_func(
            database=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT
        )
    )

    listener_task = asyncio.create_task(
        listener.run(
            {"elasticsearch_update": handle_notifications},
            policy=asyncpg_listen.ListenPolicy.LAST,
            # notification_timeout=5,
        )
    )

    try:
        # Keep the script running indefinitely to continuously listen for notifications
        while True:
            await asyncio.sleep(1)

    except asyncio.CancelledError:
        pass
    finally:
        # Cancel the listener task when done
        listener_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
