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
