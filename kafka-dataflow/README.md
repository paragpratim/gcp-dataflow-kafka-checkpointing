# Kafka Dataflow
Apache Beam/Dataflow pipeline for Kafka to BigQuery ingestion with durable checkpointing and restart-safe offset progression.
## Architecture
- Design details: [`DESIGN.md`](DESIGN.md)
- Editable processing diagram: [`docs/checkpoint-processing.excalidraw`](docs/checkpoint-processing.excalidraw)
- Quick preview diagram: [`docs/checkpoint-processing.svg`](docs/checkpoint-processing.svg)
- Project coding conventions: [`.github/copilot-instructions.md`](.github/copilot-instructions.md)
## Template Scaffolding
For quick, consistent Beam scaffolding:
- Template index: [`docs/templates/README.md`](docs/templates/README.md)
- DoFn template: [`docs/templates/NewDoFnTemplate.java`](docs/templates/NewDoFnTemplate.java)
- PTransform template: [`docs/templates/NewPTransformTemplate.java`](docs/templates/NewPTransformTemplate.java)
- DoFn test template: [`docs/templates/NewDoFnTestTemplate.java`](doc# Kafka Dataflow
Apache Beam/Dataflow pipeline for Kafka to BigQuery ingestion with durable checkpointingatApache Beam/Datem## Architecture
- Design details: [`DESIGN.cd /Users/paragghosh/vs-code-projects/gcp-dataflow-kafka-checkpointing/kafka-dataflow && mvn test -q >/tmp/kafka-dataflow-final-validation.log 2>&1; echo "EXIT_CODE=$?"; tail -40 /tmp/kafka-dataflow-final-validation.log
