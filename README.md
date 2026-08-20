# Real-Time Data Pipeline

A production-style streaming analytics project that ingests transaction events, validates them, processes them with Apache Kafka/Spark concepts, and produces operational metrics.

## Architecture

```text
Event Producer -> Kafka Topic -> Stream Processor -> Data Quality Checks -> Analytics Sink
                                      |
                                      +-> Dead Letter Queue
```

## Stack

- Python
- Apache Kafka
- PySpark Structured Streaming
- PostgreSQL / data warehouse sink
- Docker
- pytest

## Design Goals

- At-least-once event processing
- Schema validation
- Duplicate-event handling
- Error isolation through a dead-letter queue
- Partition-aware processing
- Observable processing metrics

## Repository Structure

```text
real-time-data-pipeline/
├── producer/producer.py
├── processor/stream_processor.py
├── schemas/event_schema.json
├── tests/test_processor.py
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Example Event

```json
{"event_id":"evt-1001","customer_id":"cust-42","event_type":"purchase","amount":89.95,"event_time":"2026-08-20T14:20:00Z"}
```

This repository is a portfolio implementation; credentials and production endpoints are intentionally excluded.
