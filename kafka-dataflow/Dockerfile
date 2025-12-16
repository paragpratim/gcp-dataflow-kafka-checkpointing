# Dockerfile for Dataflow Flex Template with Kafka Dataflow JAR
FROM gcr.io/dataflow-templates-base/java11-template-launcher-base:latest

# Copy the Dataflow pipeline JAR into the container
COPY target/kafka-dataflow-1.0-SNAPSHOT.jar /template/kafka-dataflow.jar

# Set the environment variable for the Flex Template launcher
ENV FLEX_TEMPLATE_JAR="/template/kafka-dataflow.jar"

# Entrypoint for Dataflow Flex Template
ENTRYPOINT ["/opt/google/dataflow/java_template_launcher", "--flexTemplateJars=/template/kafka-dataflow.jar"]
