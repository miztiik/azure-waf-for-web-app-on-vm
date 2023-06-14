import json
import logging
import datetime
import time
import os
import random
import uuid
import socket

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient

GREEN_COLOR = "\033[32m"
RED_COLOR = "\033[31m"
RESET_COLOR = "\033[0m"


# Example usage with logging
logging.info(f'{GREEN_COLOR}This is green text{RESET_COLOR}')

class GlobalArgs:
    OWNER = "Mystique"
    VERSION = "2023-05-20"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    EVNT_WEIGHTS = {"success": 80, "fail": 20}
    TRIGGER_RANDOM_FAILURES = os.getenv("TRIGGER_RANDOM_FAILURES", True)
    WAIT_SECS_BETWEEN_MSGS = int(os.getenv("WAIT_SECS_BETWEEN_MSGS", 2))
    TOT_MSGS_TO_PRODUCE = int(os.getenv("TOT_MSGS_TO_PRODUCE", 10))

    SA_NAME = os.getenv("SA_NAME", "warehouse7rfk2o005")
    BLOB_SVC_ACCOUNT_URL = os.getenv("BLOB_SVC_ACCOUNT_URL")
    BLOB_NAME = os.getenv("BLOB_NAME", "store-events-blob-005")
    BLOB_PREFIX = "sales_events"

    COSMOS_DB_URL = os.getenv("COSMOS_DB_URL", "https://warehouse-cosmos-db-005.documents.azure.com:443/")
    COSMOS_DB_NAME = os.getenv("COSMOS_DB_NAME", "warehouse-cosmos-db-005")
    COSMOS_DB_CONTAINER_NAME = os.getenv("COSMOS_DB_CONTAINER_NAME", "warehouse-cosmos-db-container-005")
    
    SVC_BUS_FQDN = os.getenv("SVC_BUS_FQDN", "warehouse-q-svc-bus-ns-002.servicebus.windows.net")
    SVC_BUS_Q_NAME = os.getenv("SVC_BUS_Q_NAME","warehouse-q-svc-bus-q-002")


def _rand_coin_flip():
    r = False
    if os.getenv("TRIGGER_RANDOM_FAILURES", True):
        if random.randint(1, 100) > 90:
            r = True
    return r

def _gen_uuid():
    return str(uuid.uuid4())

def write_to_blob(container_prefix, data: dict, blob_svc_client):
    try:
        blob_name = f"{GlobalArgs.BLOB_PREFIX}/event_type={container_prefix}/dt={datetime.datetime.now().strftime('%Y_%m_%d')}/{datetime.datetime.now().strftime('%s%f')}.json"
        resp = blob_svc_client.get_blob_client(container=f"{GlobalArgs.BLOB_NAME}", blob=blob_name).upload_blob(json.dumps(data).encode("UTF-8"))
        logging.info(f"Blob {GREEN_COLOR}{blob_name}{RESET_COLOR} uploaded successfully")
    except Exception as e:
        logging.exception(f"ERROR:{str(e)}")

def write_to_cosmosdb(data: dict, db_container):
    try:
        data["id"] = data.pop("request_id")
        resp = db_container.create_item(body=data)
        # db_container.create_item(body={'id': str(random.randrange(100000000)), 'ts': str(datetime.datetime.now())})
        logging.info(f"Document with id {GREEN_COLOR}{data['id']}{RESET_COLOR} written to CosmosDB successfully")
    except Exception as e:
        logging.exception(f"ERROR:{str(e)}")


def main(msg: func.ServiceBusMessage) -> str:
    _a_resp = {"status": False,
               "miztiik_event_processed": False}
    msg_body= msg.get_body().decode("utf-8")
    try:

        result = json.dumps({
            'message_id': msg.message_id,
            'body': msg.get_body().decode('utf-8'),
            'content_type': msg.content_type,
            'delivery_count': msg.delivery_count,
            'expiration_time': (msg.expiration_time.isoformat() if
                                msg.expiration_time else None),
            'label': msg.label,
            'partition_key': msg.partition_key,
            'reply_to': msg.reply_to,
            'reply_to_session_id': msg.reply_to_session_id,
            'scheduled_enqueue_time': (msg.scheduled_enqueue_time.isoformat() if
                                    msg.scheduled_enqueue_time else None),
            'session_id': msg.session_id,
            'time_to_live': msg.time_to_live,
            'to': msg.to,
            'user_properties': msg.user_properties,
            'event_type': msg.user_properties.get('event_type')
        })

        logging.info(f"{json.dumps(msg_body, indent=4)}")
        logging.info(f"recv_msg: {result}")

        azure_log_level = logging.getLogger("azure").setLevel(logging.ERROR)
        default_credential = DefaultAzureCredential(logging_enable=False,logging=azure_log_level)

        blob_svc_client = BlobServiceClient(GlobalArgs.BLOB_SVC_ACCOUNT_URL, credential=default_credential, logging=azure_log_level)
        
        cosmos_client = CosmosClient(url=GlobalArgs.COSMOS_DB_URL, credential=default_credential)
        db_client = cosmos_client.get_database_client(GlobalArgs.COSMOS_DB_NAME)
        db_container = db_client.get_container_client(GlobalArgs.COSMOS_DB_CONTAINER_NAME)

        # write to blob
        _evnt_type=msg.user_properties.get("event_type")
        write_to_blob(_evnt_type, json.loads(msg_body), blob_svc_client)

        # write to cosmosdb
        write_to_cosmosdb(json.loads(msg_body), db_container)
        _a_resp["status"] = True
        _a_resp["miztiik_event_processed"] = True
        _a_resp["last_processed_on"] = datetime.datetime.now().isoformat()
        logging.info(f"{GREEN_COLOR} {json.dumps(_a_resp)} {RESET_COLOR}")


    except Exception as e:
        logging.exception(f"ERROR:{str(e)}")

    logging.info( json.dumps(_a_resp, indent=4) )