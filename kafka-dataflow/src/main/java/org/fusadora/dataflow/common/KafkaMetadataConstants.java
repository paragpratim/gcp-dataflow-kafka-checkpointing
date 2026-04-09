package org.fusadora.dataflow.common;

/**
 * Kafka metadata field names attached to TableRow for handled-offset accounting.
 */
public final class KafkaMetadataConstants {

    public static final String META_KAFKA_TOPIC = "_meta_kafka_topic";
    public static final String META_KAFKA_PARTITION = "_meta_kafka_partition";
    public static final String META_KAFKA_OFFSET = "_meta_kafka_offset";

    private KafkaMetadataConstants() {
        throw new IllegalStateException("Utility class");
    }
}

