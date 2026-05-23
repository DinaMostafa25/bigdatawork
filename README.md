# Kafka Streaming Project with Docker

A real-time streaming project using Apache Kafka, Docker, and Python producers/consumers.

This project simulates an e-commerce event streaming system with multiple Kafka topics:

- orders
- payments
- user_activity

The system demonstrates:
- Kafka producers
- Kafka consumers
- Multi-topic streaming
- Event-driven architecture
- Real-time data ingestion

---

# Project Architecture

```text
orders.json --------> orders_producer.py --------> orders topic
payments.json ------> payments_producer.py -----> payments topic
user_activity.json -> user_activity_producer.py -> user_activity topic

                                              ↓

                                   multi_consumer.py
```

---

# Technologies Used

- Apache Kafka
- Docker
- Docker Compose
- Python
- kafka-python
- Kafka UI

---

# Project Structure

```text
bigdatawork/
│
├── docker-compose.yml
│
├── orders.json
├── payments.json
├── user_activity.json
│
├── producer.py
├── payments_producer.py
├── user_activity_producer.py
│
├── multi_consumer.py
│
└── README.md
```

---

# Kafka Setup

The project uses Kafka in KRaft mode with Docker.

## Start Containers

```bash
docker compose up -d
```

## Check Running Containers

```bash
docker ps
```

---

# Kafka UI

Kafka UI is available at:

```text
http://localhost:12000
```

You can:
- View topics
- View partitions
- Monitor messages
- Check consumer groups

---

# Create Kafka Topics

Enter Kafka container:

```bash
docker exec -it kafka bash
```

Create orders topic:

```bash
kafka-topics \
--bootstrap-server kafka:9092 \
--create \
--topic orders \
--partitions 3 \
--replication-factor 1
```

Create payments topic:

```bash
kafka-topics \
--bootstrap-server kafka:9092 \
--create \
--topic payments \
--partitions 3 \
--replication-factor 1
```

Create user_activity topic:

```bash
kafka-topics \
--bootstrap-server kafka:9092 \
--create \
--topic user_activity \
--partitions 3 \
--replication-factor 1
```

List topics:

```bash
kafka-topics --bootstrap-server kafka:9092 --list
```

---

# Install Python Dependencies

Install kafka-python:

```bash
py -m pip install kafka-python
```

Verify installation:

```bash
py -m pip show kafka-python
```

---

# Data Sources

## orders.json

Contains order information.

Example:

```json
{
    "order_id": 1,
    "user_id": 101,
    "customer": "Dina",
    "product": "Laptop",
    "quantity": 1,
    "price": 1200,
    "timestamp": "2026-05-23 10:00:00"
}
```

---

## payments.json

Contains payment events.

Example:

```json
{
    "payment_id": 1,
    "order_id": 1,
    "payment_method": "Credit Card",
    "payment_status": "Success",
    "amount": 1200,
    "timestamp": "2026-05-23 11:00:00"
}
```

---

## user_activity.json

Contains user activity events.

Example:

```json
{
    "activity_id": 1,
    "user_id": 101,
    "event": "login",
    "device": "mobile",
    "country": "Egypt",
    "timestamp": "2026-05-23 12:00:00"
}
```

---

# Producers

## Orders Producer

Run:

```bash
py orders_producer.py
```

Reads:
- orders.json

Publishes messages to:
- orders topic

---

## Payments Producer

Run:

```bash
py payments_producer.py
```

Reads:
- payments.json

Publishes messages to:
- payments topic

---

## User Activity Producer

Run:

```bash
py user_activity_producer.py
```

Reads:
- user_activity.json

Publishes messages to:
- user_activity topic

---

# Multi-Topic Consumer

The consumer subscribes to:
- orders
- payments
- user_activity

Run:

```bash
py multi_topic_consumer.py
```

The consumer:
- listens to multiple Kafka topics
- prints formatted streaming events
- displays topic, partition, and offset information

Example output:

```text
============================================================

TOPIC      : orders
PARTITION  : 0
OFFSET     : 12

------------------------------------------------------------

ORDER_ID      : 12
USER_ID       : 112
CUSTOMER      : Tamer
PRODUCT       : SSD
QUANTITY      : 2
PRICE         : 140
TIMESTAMP     : 2026-05-23 10:11:00

============================================================
```

---

# Kafka Concepts Practiced

This project demonstrates:

- Kafka Topics
- Partitions
- Producers
- Consumers
- Consumer Groups
- Offsets
- Event Streaming
- Multi-topic Consumption
- Real-time Data Processing

---

# Useful Kafka Commands

## List Topics

```bash
kafka-topics --bootstrap-server kafka:9092 --list
```

## Describe Topic

```bash
kafka-topics \
--bootstrap-server kafka:9092 \
--describe \
--topic orders
```

## List Consumer Groups

```bash
kafka-consumer-groups \
--bootstrap-server kafka:9092 \
--list
```

## Describe Consumer Group

```bash
kafka-consumer-groups \
--bootstrap-server kafka:9092 \
--describe \
--group analytics-group
```
# Kafka + Spark Streaming Project

## Architecture

Python Producers → Kafka → Spark Structured Streaming

## Topics

- orders
- payments
- user_activity

## Tech Stack

- Kafka (Docker)
- Kafka UI
- Python
- PySpark
- WSL Ubuntu
- JSON Streaming

## Kafka Configuration

(KRaft mode explanation)

## Producers

(explain JSON producers)

## Spark Structured Streaming

(explain Kafka integration)

## Compatibility Notes

(Java/Spark versions)

## Common Errors and Fixes

(real debugging notes)

## Future Improvements

- joins
- aggregations
- dashboards
- parquet sink
- Delta Lake

