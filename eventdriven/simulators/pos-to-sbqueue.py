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
# --- 1. CONFIGURATION (Set these in your Terminal/Environment) ---
STORAGE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")  # Use the second storage account for this simulator
SB_CONN_STR = os.getenv("AZURE_SERVICE_BUS_STD_CONNECTION_STRING")
QUEUE_NAME = os.getenv("QUEUE_NAME")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")

fake = Faker()

# --- 2. PET PRODUCT CATALOG (The "Real-World" Data) ---
PET_CATALOG = [
    {"sku": "DOG-KIB-001", "name": "Blue Buffalo Adult Chicken 30lb", "category": "Food", "sub_cat": "Dog", "price": 54.99, "uom": "LB"},
    {"sku": "CAT-LIT-055", "name": "Fresh Step Scented Litter 25lb", "category": "Basics", "sub_cat": "Cat", "price": 18.50, "uom": "LB"},
    {"sku": "DOG-TOY-992", "name": "KONG Classic Rubber Toy Large", "category": "Toys", "sub_cat": "Dog", "price": 12.99, "uom": "EA"},
    {"sku": "PET-MED-102", "name": "Frontline Plus Flea & Tick 3ct", "category": "Health", "sub_cat": "Multi", "price": 42.00, "uom": "EA"},
    {"sku": "FISH-AQ-500", "name": "Tetra Min Flakes 7oz", "category": "Food", "sub_cat": "Fish", "price": 8.75, "uom": "OZ"},
    {"sku": "BIRD-SEED-12", "name": "Kaytee Wild Bird Blend 10lb", "category": "Food", "sub_cat": "Bird", "price": 11.20, "uom": "LB"}
]

def generate_pet_pos_data():
    """Creates a realistic pet-store transaction object"""
    transaction_id = str(uuid.uuid4())
    selected_items = random.sample(PET_CATALOG, k=random.randint(1, 5))
    
    line_items = []
    subtotal = 0
    for item in selected_items:
        qty = random.randint(1, 3)
        line_price = round(item["price"] * qty, 2)
        subtotal += line_price
        line_items.append({
            "sku": item["sku"],
            "productName": item["name"],
            "category": item["category"],
            "species": item["sub_cat"],
            "quantity": qty,
            "unitPrice": item["price"],
            "uom": item["uom"],
            "lineTotal": line_price
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

def run_simulator():
    """Main execution loop for the simulator"""
    if not STORAGE_CONN_STR or not SB_CONN_STR:
        print("ERROR: Please set AZURE_STORAGE_CONNECTION_STRING and AZURE_SERVICE_BUS_CONNECTION_STRING")
        return

    # Initialize Clients
    blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
    sb_client = ServiceBusClient.from_connection_string(SB_CONN_STR)

    # Generate a random batch size (e.g., a "burst" of customers)
    num_transactions = random.randint(3, 12)
    print(f"--- Simulation Started: Processing {num_transactions} Pet Store Sales ---")
    print(f"Queue name : {QUEUE_NAME}")
    try:
        with sb_client.get_queue_sender(queue_name=QUEUE_NAME) as sender:
            for i in range(num_transactions):
                # 1. Generate realistic data
                pos_data = generate_pet_pos_data()
                print(pos_data)
                t_id = pos_data["header"]["transactionId"]

                # 2. Upload to Blob (Claim Check Pattern)
                blob_name = f"incoming/{t_id}.json"
                blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
                blob_client.upload_blob(json.dumps(pos_data), overwrite=True)

                # 3. Prepare Queue Message (The Pointer)
                pointer_payload = {
                    "version": "1.0",
                    "blobUrl": blob_client.url,
                    "storeId": pos_data["header"]["storeId"],
                    "totalAmount": pos_data["summary"]["total"],
                    "eventTime": pos_data["header"]["timestamp"]
                }
                
                message = ServiceBusMessage(json.dumps(pointer_payload))
                message.application_properties = {"StoreId": pos_data["header"]["storeId"]}

                # 4. Send to Service Bus
                sender.send_messages(message)
                print(f"[SUCCESS] {i+1}/{num_transactions}: Store {pos_data['header']['storeId']} - Total ${pos_data['summary']['total']}")
                print(message)
                # Small delay to simulate real-world processing
                time.sleep(random.uniform(0.2, 0.8))

    except Exception as e:
        print(f"[FATAL ERROR] Simulation failed: {e}")
    finally:
        sb_client.close()

if __name__ == "__main__":
    run_simulator()