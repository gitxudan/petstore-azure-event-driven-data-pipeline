
import os
import json
import uuid
import random
import time
from datetime import datetime
from faker import Faker
from azure.storage.blob import BlobServiceClient
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
STORAGE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
SB_CONN_STR = os.getenv("AZURE_SERVICE_BUS_STD_CONNECTION_STRING")
TOPIC_NAME = "operation-topics"
CONTAINER_NAME = os.getenv("CONTAINER_NAME")

fake = Faker()


PET_CATALOG = [
    {"sku": "DOG-KIB-001", "name": "Blue Buffalo Adult Chicken 30lb", "category": "Food", "sub_cat": "Dog", "price": 54.99, "uom": "LB"},
    {"sku": "CAT-LIT-055", "name": "Fresh Step Scented Litter 25lb", "category": "Basics", "sub_cat": "Cat", "price": 18.50, "uom": "LB"},
    {"sku": "DOG-TOY-992", "name": "KONG Classic Rubber Toy Large", "category": "Toys", "sub_cat": "Dog", "price": 12.99, "uom": "EA"},
    {"sku": "PET-MED-102", "name": "Frontline Plus Flea & Tick 3ct", "category": "Health", "sub_cat": "Multi", "price": 42.00, "uom": "EA"},
    {"sku": "FISH-AQ-500", "name": "Tetra Min Flakes 7oz", "category": "Food", "sub_cat": "Fish", "price": 8.75, "uom": "OZ"},
    {"sku": "BIRD-SEED-12", "name": "Kaytee Wild Bird Blend 10lb", "category": "Food", "sub_cat": "Bird", "price": 11.20, "uom": "LB"}
]

# ------------------------------
# 3️⃣ GENERATE POS TRANSACTION
# ------------------------------
def generate_pet_pos_data():
    transaction_id = str(uuid.uuid4())
    selected_items = random.sample(PET_CATALOG, k=random.randint(1, 5))

    line_items = []
    subtotal = 0

    for item in selected_items:
        qty = random.randint(1, 3)
        line_total = round(item["price"] * qty, 2)
        subtotal += line_total

        line_items.append({
            "sku": item["sku"],
            "productName": item["name"],
            "category": item["category"],
            "species": item["sub_cat"],
            "quantity": qty,
            "unitPrice": item["price"],
            "uom": item["uom"],
            "lineTotal": line_total
        })

    tax = round(subtotal * 0.07, 2)

    return {
        "header": {
            "transactionId": transaction_id,
            "storeId": f"PET-STORE-{random.randint(50, 60)}",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "loyaltyCardId": f"LOY-{random.randint(100000, 999999)}" if random.random() > 0.5 else None
        },
        "items": line_items,
        "summary": {
            "subtotal": round(subtotal, 2),
            "tax": tax,
            "total": round(subtotal + tax, 2)
        }
    }

# ------------------------------
# 4️⃣ MAIN SIMULATOR
# ------------------------------
def run_simulator():

    if not STORAGE_CONN_STR or not SB_CONN_STR:
        print("❌ Missing connection strings in .env")
        return

    print("🚀 Starting Operational Broadcast Simulator")
    print(f"📌 Topic: {TOPIC_NAME}")
    print(f"📦 Container: {CONTAINER_NAME}")

    blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
    sb_client = ServiceBusClient.from_connection_string(SB_CONN_STR)

    num_transactions = random.randint(5, 15)

    try:
        with sb_client.get_topic_sender(topic_name=TOPIC_NAME) as sender:

            for i in range(num_transactions):

                # 1️⃣ Generate POS data
                pos_data = generate_pet_pos_data()
                transaction_id = pos_data["header"]["transactionId"]

                # 2️⃣ Upload full payload to Blob (Claim Check)
                blob_name = f"incoming/{transaction_id}.json"
                blob_client = blob_service_client.get_blob_client(
                    container=CONTAINER_NAME,
                    blob=blob_name
                )

                blob_client.upload_blob(
                    json.dumps(pos_data),
                    overwrite=True
                )

                # 3️⃣ Create Event Envelope (Fan-out)
                event_payload = {
                    "eventType": "PetSaleOccurred",
                    "eventId": str(uuid.uuid4()),
                    "eventTime": datetime.utcnow().isoformat() + "Z",
                    "storeId": pos_data["header"]["storeId"],
                    "data": {
                        "blobUrl": blob_client.url,
                        "totalAmount": pos_data["summary"]["total"]
                    }
                }

                message = ServiceBusMessage(
                    json.dumps(event_payload)
                )

                # Optional metadata (for subscription filters if needed)
                message.subject = "PetSaleOccurred"
                message.application_properties = {
                    "EventType": "PetSaleOccurred"
                }

                # 4️⃣ Broadcast to Topic
                sender.send_messages(message)

                print(
                    f"✅ {i+1}/{num_transactions} | "
                    f"{pos_data['header']['storeId']} | "
                    f"${pos_data['summary']['total']}"
                )

                time.sleep(random.uniform(0.3, 1.0))

        print("🎯 Broadcast simulation completed successfully.")

    except Exception as e:
        print(f"❌ Simulation failed: {e}")

    finally:
        sb_client.close()

# ------------------------------
# ENTRY POINT
# ------------------------------
if __name__ == "__main__":
    run_simulator()
