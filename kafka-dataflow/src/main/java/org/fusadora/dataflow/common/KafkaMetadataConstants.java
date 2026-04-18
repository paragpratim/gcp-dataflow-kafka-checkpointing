package org.fusadora.dataflow.common;

/**
 * Kafka metadata field names attached to TableRow for handled-offset accounting.
 */
public final class KafkaMetadataConstants {

    public static final String META_KAFKA_TOPIC = "topic";
    public static final String META_KAFKA_PARTITION = "partition";
    public static final String META_KAFKA_OFFSET = "offset";

    private KafkaMetadataConstants() {
        throw new IllegalStateException("Utility class");
    }
}

