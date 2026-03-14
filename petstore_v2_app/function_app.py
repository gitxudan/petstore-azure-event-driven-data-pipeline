import azure.functions as func
import datetime
import json
import logging
import os
from azure.eventgrid import EventGridPublisherClient
from azure.core.credentials import AzureKeyCredential
from azure.core.messaging import CloudEvent

from azure.storage.blob import BlobServiceClient
from urllib.parse import urlparse

import pymssql
import dateutil.parser

app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="azservicebus", 
                               queue_name="testqueue", 
                               connection="ServiceBusConnectionString")
def queue_handler_finance(azservicebus: func.ServiceBusMessage):
    
    try:
        # 1️⃣ Read message
        message_body = azservicebus.get_body().decode("utf-8")
        logging.info(f"ServiceBus message: {message_body}")

        msg_json = json.loads(message_body)
        blob_url = msg_json["blobUrl"]

        # 2️⃣ Parse URL
        parsed = urlparse(blob_url)

        path_parts = parsed.path.lstrip("/").split("/", 1)
        container_name = path_parts[0]
        blob_name = path_parts[1]

        logging.info(f"Container: {container_name}")
        logging.info(f"Blob: {blob_name}")

        # 3️⃣ Create storage client using key / connection string
        conn_str = os.environ["AzureWebJobsStorage"]

        blob_service_client = BlobServiceClient.from_connection_string(conn_str)

        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_name
        )

        # 4️⃣ Download blob
        blob_data = blob_client.download_blob().readall()

        # 5️⃣ Print blob content
        logging.info("Blob content retrieved:")
        logging.info(blob_data.decode("utf-8"))

    except Exception as e:
        logging.error("Failed processing message")
        logging.error(str(e))
    
        # ---------------- existing logics --------#
    # convert bytes -> string -> dict
    data = json.loads(blob_data.decode("utf-8"))

    # now you can access fields
    mission_event = CloudEvent(
        source="/azure/functions/petstore",
        type="Petstore.TransactionCompleted",
        subject=f"petstore/stores/{data['header']['storeId']}/transactions/{data['header']['transactionId']}",
        data={
            "transactionId": data["header"]["transactionId"],
            "storeId": data["header"]["storeId"],
            "timestamp": data["header"]["timestamp"],
            "loyaltyCardId": data["header"]["loyaltyCardId"],
            "totalAmount": data["summary"]["total"],
            "items": data["items"]
        }
    )
    endpoint = os.environ["EGCustomTopicEndpointUri"]
    key = os.environ["EGCustomTopicKeySetting"]
    client = EventGridPublisherClient(
    endpoint,
    AzureKeyCredential(key)
    )
    # 3. Fire it!
    client.send(mission_event)

    
@app.service_bus_topic_trigger(
    arg_name="azservicebus",
    topic_name="operation-topics",
    subscription_name="marketing-sub",
    connection="ServiceBusConnectionString"
)
def topic_handler_operation(azservicebus: func.ServiceBusMessage):

    message_body = azservicebus.get_body().decode("utf-8")
    logging.info("ServiceBus Topic message received:")
    logging.info(message_body)
    

 
@app.event_grid_trigger(arg_name="event")
def EG_handler_blob_created(event: func.EventGridEvent):
    logging.info("Event Grid event received:")
    logging.info(f"Event ID: {event.id}")
    logging.info(f"Event Type: {event.event_type}")
    logging.info(f"Event Subject: {event.subject}") 
    logging.info(f"Event Data: {json.dumps(event.get_json())}")
    

#[SQL_SERVER,SQL_USER,SQL_PASSWORD,SQL_DATABASE] should be setup in the functionapp settings as environment variables, not hardcoded in code


@app.event_grid_trigger(arg_name="event")
def EG_handler_ct_mission_complete(event: func.EventGridEvent):
    """
    Event Grid handler that inserts the received transaction directly into Azure SQL.
    """
    logging.info("Event Grid Mission Complete event received")

    # 1️⃣ Parse event data
    try:
        data = event.get_json()
        transactionId = data["transactionId"]
        storeId = data["storeId"]
        loyaltyCardId = data.get("loyaltyCardId")  # optional
        timestamp = dateutil.parser.isoparse(data["timestamp"])
        totalAmount = data["totalAmount"]
        items_json = json.dumps(data["items"])
    except Exception as e:
        logging.error(f"Failed to parse event data: {e}")
        return

    # 2️⃣ Connect to Azure SQL and insert
    conn = None
    cursor = None
    try:
        conn = pymssql.connect(
            server=os.environ["SQL_SERVER"],
            user=os.environ["SQL_USER"],
            password=os.environ["SQL_PASSWORD"],
            database=os.environ["SQL_DATABASE"]
        )
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO Transactions
            (transactionId, storeId, loyaltyCardId, eventTimestamp, totalAmount, items)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (transactionId, storeId, loyaltyCardId, timestamp, totalAmount, items_json))
        conn.commit()
        logging.info(f"Transaction {transactionId} inserted into Azure SQL")
    except Exception as e:
        logging.error(f"Failed to insert transaction {transactionId} into SQL: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()