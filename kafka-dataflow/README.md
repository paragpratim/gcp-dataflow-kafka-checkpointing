# Kafka Dataflow

Apache Beam/Dataflow pipeline for Kafka to BigQuery ingestion with durable checkpointing and restart-safe offset progression.

## Architecture

- Design details: [`DESIGN.md`](DESIGN.md)
- Updated checkpoint flow diagram (Mermaid): [`docs/checkpoint-processing.md`](docs/checkpoint-processing.md)
- Legacy editable diagram: [`docs/checkpoint-processing.excalidraw`](docs/checkpoint-processing.excalidraw)
- Legacy preview diagram: [`docs/checkpoint-processing.svg`](docs/checkpoint-processing.svg)
- Project coding conventions: [`.github/copilot-instructions.md`](.github/copilot-instructions.md)

## Template Scaffolding

For quick, consistent Beam scaffolding:

- Template index: [`docs/templates/README.md`](docs/templates/README.md)
- DoFn template: [`docs/templates/NewDoFnTemplate.java`](docs/templates/NewDoFnTemplate.java)
- PTransform template: [`docs/templates/NewPTransformTemplate.java`](docs/templates/NewPTransformTemplate.java)
- DoFn test template: [`docs/templates/NewDoFnTestTemplate.java`](docs/templates/NewDoFnTestTemplate.java)
