# Cloud Architecture Sketch

## Databricks-oriented production design

1. Source files arrive in object storage or are posted through an API.
2. A producer publishes raw trip events to Kafka.
3. Databricks Structured Streaming consumes the topic into a Bronze Delta table.
4. Validation and standardization write to Silver.
5. Grouped trip summaries and weekly aggregates are written to Gold.
6. Curated data is served to downstream SQL consumers and the FastAPI service.
7. Monitoring and lineage are handled through platform-native observability and catalog tooling.

## Recommended managed services

- Kafka: Confluent Cloud or cloud-native equivalent
- Spark: Databricks Jobs / Structured Streaming
- Storage: S3 / ADLS / GCS with Delta Lake
- Serving DB: PostgreSQL / managed relational database
- API: container platform such as ECS, Cloud Run, AKS, or GKE
