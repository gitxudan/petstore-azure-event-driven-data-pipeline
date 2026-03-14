# petstore-azure-event-driven-data-pipeline

**Mini-project demonstrating an end-to-end cloud event-driven data pipeline using Azure.**

---

## Overview

This project simulates a **pet store point-of-sale (POS) system** and demonstrates an **event-driven data pipeline** on Azure. Transactions are generated, uploaded to Blob Storage, sent to a Service Bus queue, and finally processed by Azure Functions, which store the data into **Azure SQL Database**.  

It showcases **cloud-native patterns** such as:

- Event-driven architecture
- Claim Check pattern with Azure Blob Storage
- Serverless compute with Azure Functions
- Integration between **Azure Event Grid** and **Service Bus**
- Cloud-based mini-project ready for hands-on practice

---

## Architecture

```text
[POS Simulator] ---> [Blob Storage] ---> [Service Bus Queue] ---> [Azure Function] ---> [Event Grid] ---> [Azure SQL Database]

POS Simulator
Generates random pet store transactions and uploads them to Azure Blob Storage. Sends a pointer message to Service Bus.

Azure Blob Storage
Holds transaction JSON files (Claim Check pattern) for decoupling and durability.

Azure Service Bus Queue
Receives transaction pointer messages to trigger processing.

Azure Function

Listens to Service Bus messages

Reads the Blob data

Publishes an Event Grid event

Inserts transactions into Azure SQL Database

Azure SQL Database
Stores processed transactions for analytics and reporting.

Features

End-to-end cloud data pipeline simulation

Event-driven processing with Azure Functions and Event Grid

Storing JSON transaction data into SQL

Service Bus integration for reliable messaging

Claim Check pattern using Blob Storage

Python implementation with pymssql and azure SDKs

Tech Stack
Layer	Technology
Cloud Storage	Azure Blob Storage
Messaging	Azure Service Bus Queue & Topic
Serverless Compute	Azure Functions (Python)
Event Routing	Azure Event Grid
Database	Azure SQL Database
Language / SDK	Python, azure-storage-blob, azure-servicebus, pymssql
Simulation	faker library for POS data
Folder Structure
petstore-azure-event-driven-data-pipeline/
├─ eventdriven/                # Azure Functions app
│  ├─ __init__.py
│  ├─ function_app.py          # Main triggers & handlers
├─ simulators/                 # POS simulator scripts
│  ├─ pos_to_queue.py
│  ├─ .env.example
├─ README.md
├─ .gitignore
Setup & Usage

Clone the repository

git clone https://github.com/gitxudan/petstore-azure-event-driven-data-pipeline.git
cd petstore-azure-event-driven-data-pipeline

Create .env from example

cp simulators/.env.example simulators/.env
# Fill in your Azure credentials & settings

Run POS Simulator

python simulators/pos_to_queue.py

Deploy Azure Function App (event-driven handlers)

# Ensure you have Azure Functions Core Tools installed
func azure functionapp publish <YourFunctionAppName>

Check Azure SQL Database
Transactions should appear in the Transactions table.

Notes

Secrets: .env should never be pushed to GitHub. Use .env.example instead.

Event-Driven: Event Grid events can be used for further processing, analytics, or triggering downstream pipelines.

Extensible: You can expand the simulator, add more services (Power BI, Databricks), or replicate the pipeline in AWS/GCP as mini-projects.
