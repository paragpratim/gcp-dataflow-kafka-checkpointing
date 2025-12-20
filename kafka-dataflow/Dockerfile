# Dockerfile for Dataflow Flex Template with Kafka Dataflow JAR (recommended pattern)
FROM gcr.io/dataflow-templates-base/java17-template-launcher-base:latest

# Copy the Dataflow pipeline JAR into the container
COPY target/kafka-dataflow-1.0-SNAPSHOT.jar /template/kafka-dataflow.jar

# Set required environment variables for Flex Template Java launcher
ENV FLEX_TEMPLATE_JAVA_MAIN_CLASS="org.fusadora.dataflow.pipelines.PipelineRunner"
ENV FLEX_TEMPLATE_JAVA_CLASSPATH="/template/kafka-dataflow.jar"
