# Repository Description
This repository contains the code for the kafka-dataflow java project, which is a data processing application built using Apache Beam and Google Cloud Dataflow. The project ingests data from Kafka, processes it using Beam pipelines, and writes the results to BigQuery. 


# Architectural and Coding Conventions
The codebase follows specific architectural and coding conventions to ensure maintainability, testability, and scalability. The repository includes layered package structures, clear separation of concerns, and consistent handling of checkpoints, offsets, and payloads. It also emphasizes the use of constants, configuration utilities, and reusable components to promote code quality and reduce duplication.


## Project Structure
- `kafka-dataflow/`: Contains the main application code organized into layered packages.
- `kafka-example-client/`: Contains a simple CLI client for testing and demonstration purposes.
- `terraform/`: Contains Terraform scripts for infrastructure provisioning.
- `scripts/`: Contains utility scripts for deployment and management.
- `docker-compose.yaml`: Docker Compose configuration for local development and testing.
- `.gitleaks.toml`: Configuration for detecting sensitive information in the codebase.



