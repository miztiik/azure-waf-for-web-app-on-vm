import json
import logging
import datetime
import time
import os
import random
import uuid
import socket

import azure.functions as func
from azure.servicebus import ServiceBusClient, ServiceBusMessage

from azure.identity import DefaultAzureCredential

GREEN_COLOR = "\033[32m"
RED_COLOR = "\033[31m"
RESET_COLOR = "\033[0m"


# Example usage with logging
logging.info(f'{GREEN_COLOR}This is green text{RESET_COLOR}')

class GlobalArgs:
    OWNER = "Mystique"
    VERSION = "2023-05-21"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    EVNT_WEIGHTS = {"success": 80, "fail": 20}
    TRIGGER_RANDOM_FAILURES = os.getenv("TRIGGER_RANDOM_FAILURES", True)
    WAIT_SECS_BETWEEN_MSGS = int(os.getenv("WAIT_SECS_BETWEEN_MSGS", 2))
    TOT_MSGS_TO_PRODUCE = int(os.getenv("TOT_MSGS_TO_PRODUCE", 10))
    SVC_BUS_CONNECTION_STR = os.getenv("SVC_BUS_CONNECTION_STR")
    SVC_BUS_FQDN = os.getenv("SVC_BUS_FQDN", "warehouse-q-svc-bus-ns-002.servicebus.windows.net")
    SVC_BUS_Q_NAME = os.getenv("SVC_BUS_Q_NAME","warehouse-q-svc-bus-q-002")
    SVC_BUS_TOPIC_NAME = os.getenv("SVC_BUS_TOPIC_NAME")

def _rand_coin_flip():
    r = False
    if os.getenv("TRIGGER_RANDOM_FAILURES", True):
        if random.randint(1, 100) > 90:
            r = True
    return r

def _gen_uuid():
    return str(uuid.uuid4())

def evnt_producer():
    resp = {"status": False}

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
                "id": _u,
                "request_id": _u,
                "store_id": random.randint(1, 10),
                "store_fqdn": str(store_fqdn),
                "store_ip": str(store_ip),
                "cust_id": random.randint(100, 999),
                "category": random.choice(_categories),
                "sku": random.randint(18981, 189281),
                "price": _s,
                "qty": random.randint(1, 38),
                "discount": random.randint(0, 75),
                "gift_wrap": bool(random.getrandbits(1)),
                "variant": random.choice(_variants),
                "priority_shipping": p_s,
                "ts": datetime.datetime.now().isoformat(),
                "contact_me": "github.com/miztiik"
            }
            _attr = {
                "discount": evnt_body["discount"],
                "event_type": _evnt_type,
                "priority_shipping": f"{p_s}"
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

            logging.info(f"generated_event:{json.dumps(evnt_body)}")

            # Upload to Blob Storage
            # outputBlob.set(str(evnt_body)) # Imperative to type cast to str
            # logging.info(f"Uploaded to blob storage")

            # # Injest to CosmosDB
            # doc.set(func.Document.from_json(json.dumps(evnt_body)))
            # logging.info('Document injestion success')

            # Write To Service Bus Queue
            write_to_svc_bus_q(evnt_body, _attr)            
            
            # Write To Service Bus Topic
            write_to_svc_bus_topic(evnt_body, _attr)

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
        # logging.info(f'{GREEN_COLOR} {{"resp":{json.dumps(resp)}}} {RESET_COLOR}')

    except Exception as e:
        logging.error(f"ERROR:{str(e)}")
        resp["err_msg"] = str(e)

    return resp

def _get_n_set_app_config(credential):
    try:
        GlobalArgs.APP_CONFIG_URL= f"https://{GlobalArgs.APP_CONFIG_NAME}.azconfig.io"

        client = AzureAppConfigurationClient(GlobalArgs.APP_CONFIG_URL, credential=credential)
        
        GlobalArgs.SA_NAME = client.get_configuration_setting(key="saName").value
        GlobalArgs.BLOB_NAME = client.get_configuration_setting(key="blobName").value
        GlobalArgs.Q_NAME= client.get_configuration_setting(key="queueName").value

        GlobalArgs.BLOB_SVC_ACCOUNT_URL= f"https://{GlobalArgs.SA_NAME}.blob.core.windows.net"
        GlobalArgs.Q_SVC_ACCOUNT_URL= f"https://{GlobalArgs.SA_NAME}.queue.core.windows.net"
    except Exception as e:
        logging.exception(f"ERROR:{str(e)}")

def write_to_svc_bus_q(data, _attr):
    # Setup up Azure Credentials
    azure_log_level = logging.getLogger("azure").setLevel(logging.ERROR)
    credential = DefaultAzureCredential(logging_enable=False,logging=azure_log_level)

    with  ServiceBusClient(GlobalArgs.SVC_BUS_FQDN, credential=credential) as client:
        with client.get_queue_sender(GlobalArgs.SVC_BUS_Q_NAME) as sender:
            # Sending a single message
            msg_to_send = ServiceBusMessage(
                json.dumps(data),
                time_to_live = datetime.timedelta(days=1),
                application_properties=_attr
            )
            
            _r = sender.send_messages(msg_to_send)
            logging.debug(f"Message sent: {json.dumps(_r)}")

def write_to_svc_bus_topic(data, _attr):
    # Setup up Azure Credentials
    azure_log_level = logging.getLogger("azure").setLevel(logging.ERROR)
    credential = DefaultAzureCredential(logging_enable=False,logging=azure_log_level)

    with  ServiceBusClient(GlobalArgs.SVC_BUS_FQDN, credential=credential) as client:
        with client.get_topic_sender(topic_name=GlobalArgs.SVC_BUS_TOPIC_NAME) as sender:
            # Sending a single message
            msg_to_send = ServiceBusMessage(
                json.dumps(data),
                time_to_live = datetime.timedelta(days=1),
                application_properties=_attr
            )
            
            _r = sender.send_messages(msg_to_send)
            logging.debug(f"Message sent: {json.dumps(_r)}")



def main(req: func.HttpRequest) -> func.HttpResponse:
    recv_cnt={}
    req_body={}
    _d={
        "miztiik_event_processed": False,
        "msg": ""
    }

    # Setup Azure Clients
    # azure_log_level = logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.ERROR) 

    # Get Config data from App Config
    # _get_n_set_app_config(credential)

    try:
        try:
            recv_cnt = req.params.get("count")
            if recv_cnt:
                recv_cnt = int(recv_cnt)
            logging.info(f"got from params: {recv_cnt}")
        except ValueError:
            pass
        if not recv_cnt:
            try:
                req_body = req.get_json()
            except ValueError:
                _d["msg"] = "count not found in body"
                logging.info("count not found in body")
                pass
            else:
                recv_cnt = int(req_body.get("count"))

        logging.info(f"Received Event: {recv_cnt}")

        if recv_cnt:
            GlobalArgs.TOT_MSGS_TO_PRODUCE = recv_cnt
    
        resp = evnt_producer()
        _d["resp"] = resp
        _d["count"] = GlobalArgs.TOT_MSGS_TO_PRODUCE
        _d["miztiik_event_processed"] = True
        _d["last_processed_on"] = datetime.datetime.now().isoformat()
        _d["msg"] = f"Generated {GlobalArgs.TOT_MSGS_TO_PRODUCE} messages"
        logging.info(f"{GREEN_COLOR} {json.dumps(_d)} {RESET_COLOR}")
    except Exception as e:
        logging.exception(f"ERROR:{str(e)}")
    
    return func.HttpResponse(
        f"{json.dumps(_d)}",
            status_code=200
    )
