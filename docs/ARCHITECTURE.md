# Architecture

```text
Event Producer
      |
      v
Apache Kafka topic
      |
      v
PySpark Structured Streaming
      |
      +--> schema validation
      +--> timestamp parsing
      +--> duplicate-event protection
      |
      v
Streaming sink / analytics layer
```

Kafka provides the event transport layer while Spark handles scalable stream processing. The repository's Docker Compose setup provides a local Kafka environment; production deployments should use managed infrastructure, authentication, TLS, monitoring, and durable checkpoint storage.
