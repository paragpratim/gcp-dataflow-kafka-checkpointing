# GCP Dataflow Kafka Checkpointing

This repository provides a robust Apache Beam/Dataflow pipeline for ingesting data from Kafka to BigQuery, featuring:

- Durable, externalized checkpointing (Firestore)
- Restart-safe, monotonic offset progression

- At-least-once delivery, with explicit handling of source gaps and invalid payloads
- Includes a pipeline to generate random records and push to Kafka (useful for testing)

## Project Structure

- **Pipeline code & docs:** See [kafka-dataflow/README.md](kafka-dataflow/README.md)
- **Design & architecture:** See [kafka-dataflow/DESIGN.md](kafka-dataflow/DESIGN.md)
- **Deployment scripts:** See [scripts/](scripts/)
- **Terraform modules:** See [terraform/](terraform/)

## Quick Start

1. Review the [kafka-dataflow/README.md](kafka-dataflow/README.md) for build and run instructions.
2. Use the provided scripts and templates for consistent deployment.
3. See the design docs for checkpointing and offset handling details.
