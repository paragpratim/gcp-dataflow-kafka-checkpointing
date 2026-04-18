package org.fusadora.dataflow.common;

/**
 * org.fusadora.dataflow.common.BigquerySchemaConstants
 * Constants for BigQuery fields.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public class BigquerySchemaConstants {
    public static final String SCHEMA_VERSION = "version";

    public static final String SCHEMA_RAW_MESSAGE = "raw_message";
    public static final String SCHEMA_KAFKA_TOPIC = "kafka_topic";
    public static final String SCHEMA_METADATA_RECORD = "__metadata";

    private BigquerySchemaConstants() {
        throw new IllegalStateException("Utility class");
    }
}

