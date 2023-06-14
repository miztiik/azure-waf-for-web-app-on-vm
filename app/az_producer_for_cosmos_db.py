import json
import logging
import datetime
import time
import os
import random
import uuid
import socket

from azure.identity import DefaultAzureCredential
from azure.appconfiguration import AzureAppConfigurationClient
from azure.storage.queue import QueueServiceClient
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.cosmos import CosmosClient

GREEN_COLOR = "\033[32m"
RED_COLOR = "\033[31m"
RESET_COLOR = "\033[0m"

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Example usage with logger
logger.info(f'{GREEN_COLOR}This is green text{RESET_COLOR}')

class GlobalArgs:
    OWNER = "Mystique"
    VERSION = "2023-05-10"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    EVNT_WEIGHTS = {"success": 80, "fail": 20}
    WAIT_SECS_BETWEEN_MSGS = int(os.getenv("WAIT_SECS_BETWEEN_MSGS", 5))
    TOT_MSGS_TO_PRODUCE = int(os.getenv("TOT_MSGS_TO_PRODUCE", 10))

    LOCATION = os.getenv("LOCATION", "westeurope")
    SA_NAME = os.getenv("SA_NAME", "warehousei5chd4011")
    BLOB_NAME = os.getenv("BLOB_NAME", "store-events-015")
    BLOB_PREFIX = "sales_events"

    APP_CONFIG_NAME=os.getenv("APP_CONFIG_NAME", "store-events-config-ibihnz-003")


def set_logging(lv=GlobalArgs.LOG_LEVEL, log_filename="/var/log/miztiik.json"):
    logging.basicConfig(level=lv)
    logger = logging.getLogger()
    logger.setLevel(lv)
    # Generate log filename with desired format
    log_filename = f"/var/log/miztiik-store-events-producer-{datetime.datetime.now().strftime('%Y-%m-%d')}.json"
    # Add a FileHandler to write logs to a file
    fh = logging.FileHandler(log_filename)
    fh.setLevel(lv)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger

logger = set_logging()


def _rand_coin_flip():
    r = False
    if os.getenv("TRIGGER_RANDOM_FAILURES", True):
        if random.randint(1, 100) > 90:
            r = True
    return r

def _gen_uuid():
    return str(uuid.uuid4())

def _get_n_set_app_config(credential):
    try:
        GlobalArgs.APP_CONFIG_URL= f"https://{GlobalArgs.APP_CONFIG_NAME}.azconfig.io"

        app_config_client = AzureAppConfigurationClient(GlobalArgs.APP_CONFIG_URL, credential=credential)
        
        GlobalArgs.SA_NAME = app_config_client.get_configuration_setting(key="SA_NAME").value
        GlobalArgs.BLOB_NAME = app_config_client.get_configuration_setting(key="BLOB_NAME").value
        GlobalArgs.Q_NAME = app_config_client.get_configuration_setting(key="Q_NAME").value

        GlobalArgs.COSMOS_DB_ACCOUNT = app_config_client.get_configuration_setting(key="COSMOS_DB_ACCOUNT").value
        GlobalArgs.COSMOS_DB_NAME = app_config_client.get_configuration_setting(key="COSMOS_DB_NAME").value
        GlobalArgs.COSMOS_DB_CONTAINER_NAME = app_config_client.get_configuration_setting(key="COSMOS_DB_CONTAINER_NAME").value

        GlobalArgs.COSMOS_DB_URL = f"https://{GlobalArgs.COSMOS_DB_ACCOUNT}.documents.azure.com"
        GlobalArgs.BLOB_SVC_ACCOUNT_URL= f"https://{GlobalArgs.SA_NAME}.blob.core.windows.net"
        GlobalArgs.Q_SVC_ACCOUNT_URL= f"https://{GlobalArgs.SA_NAME}.queue.core.windows.net"

        logger.info(f"{GREEN_COLOR}Succesfully retrieved App Configs{RESET_COLOR}")

    except Exception as e:
        logger.exception(f"ERROR:{str(e)}")
        logger.exception(f"{RED_COLOR}ERROR:{str(e)}{RESET_COLOR}")

def write_to_blob(container_prefix ,data, blob_svc_client):
    try:
        blob_name = f"{GlobalArgs.BLOB_PREFIX}/event_type={container_prefix}/dt={datetime.datetime.now().strftime('%Y_%m_%d')}/{datetime.datetime.now().strftime('%s%f')}.json"
        resp = blob_svc_client.get_blob_client(container=f"{GlobalArgs.BLOB_NAME}", blob=blob_name).upload_blob(json.dumps(data).encode("UTF-8"))
        logger.info(f"Blob {GREEN_COLOR}{blob_name}{RESET_COLOR} uploaded successfully")
    except Exception as e:
        logger.exception(f"ERROR:{str(e)}")

def write_to_cosmosdb(data, db_container):
    try:
        data["id"] = data.pop("request_id")
        resp = db_container.create_item(body=data)
        # db_container.create_item(body={'id': str(random.randrange(100000000)), 'ts': str(datetime.datetime.now())})
        logger.info(f"Document with id {GREEN_COLOR}{data['id']}{RESET_COLOR} written to CosmosDB successfully")
    except Exception as e:
        logger.exception(f"ERROR:{str(e)}")

def lambda_handler(event, context):
    resp = {"status": False}
    logger.debug(f"Event: {json.dumps(event)}")

    # Setup Azure Clients
    # azure_log_level = logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.ERROR) 
    azure_log_level = logging.getLogger("azure").setLevel(logging.ERROR)
    default_credential = DefaultAzureCredential(logging_enable=False,logging=azure_log_level)

    # Get Config data from App Config
    _get_n_set_app_config(default_credential)

    blob_svc_client = BlobServiceClient(GlobalArgs.BLOB_SVC_ACCOUNT_URL, credential=default_credential, logging=azure_log_level)
    q_svc_client = QueueServiceClient(GlobalArgs.Q_SVC_ACCOUNT_URL, credential=default_credential, logging=azure_log_level)


    cosmos_client = CosmosClient(url=GlobalArgs.COSMOS_DB_URL, credential=default_credential)
    db_client = cosmos_client.get_database_client(GlobalArgs.COSMOS_DB_NAME)
    db_container = db_client.get_container_client(GlobalArgs.COSMOS_DB_CONTAINER_NAME)

    _categories = ["Books", "Games", "Mobiles", "Groceries", "Shoes", "Stationaries", "Laptops", "Tablets", "Notebooks", "Camera", "Printers", "Monitors", "Speakers", "Projectors", "Cables", "Furniture"]
    _evnt_types = ["sale_event", "inventory_event"]
    _variants = ["black", "red"]

    try:
        t_msgs = 0
        p_cnt = 0
        s_evnts = 0
        inventory_evnts = 0
        t_sales = 0
        store_fqdn = socket.getfqdn()
        store_ip = socket.gethostbyname(socket.gethostname())
        while True:
            _s = round(random.random() * 100, 2)
            _evnt_type = random.choice(_evnt_types)
            _u = _gen_uuid()
            p_s = bool(random.getrandbits(1))
            evnt_body = {
                "request_id": _u,
                "store_id": random.randint(1, 10),
                "store_fqdn": str(store_fqdn),
                "store_ip": str(store_ip),
                "cust_id": random.randint(100, 999),
                "category": random.choice(_categories),
                "sku": random.randint(18981, 189281),
                "price": _s,
                "qty": random.randint(1, 38),
                "discount": round(random.random() * 20, 1),
                "gift_wrap": bool(random.getrandbits(1)),
                "variant": random.choice(_variants),
                "priority_shipping": p_s,
                "ts": datetime.datetime.now().isoformat(),
                "contact_me": "github.com/miztiik"
            }
            _attr = {
                "event_type": {
                    "DataType": "String",
                    "StringValue": _evnt_type
                },
                "priority_shipping": {
                    "DataType": "String",
                    "StringValue": f"{p_s}"
                }
            }

            # Make order type return
            if bool(random.getrandbits(1)):
                evnt_body["is_return"] = True

            if _rand_coin_flip():
                evnt_body.pop("store_id", None)
                evnt_body["bad_msg"] = True
                p_cnt += 1

            if _evnt_type == "sale_event":
                s_evnts += 1
            elif _evnt_type == "inventory_event":
                inventory_evnts += 1

            # write to blob
            logger.info(f"{json.dumps(evnt_body, indent=4)}")
            write_to_blob(_evnt_type, evnt_body, blob_svc_client)

            # write to cosmosdb
            write_to_cosmosdb(evnt_body, db_container)

            t_msgs += 1
            t_sales += _s
            time.sleep(GlobalArgs.WAIT_SECS_BETWEEN_MSGS)
            if t_msgs >= GlobalArgs.TOT_MSGS_TO_PRODUCE:
                break

        resp["tot_msgs"] = t_msgs
        resp["bad_msgs"] = p_cnt
        resp["sale_evnts"] = s_evnts
        resp["inventory_evnts"] = inventory_evnts
        resp["tot_sales"] = t_sales
        resp["status"] = True
        logger.info(f'{GREEN_COLOR} {{"resp":{json.dumps(resp)}}} {RESET_COLOR}')

    except Exception as e:
        logger.error(f"ERROR:{str(e)}")
        resp["err_msg"] = str(e)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": resp
        })
    }


lambda_handler({}, {})