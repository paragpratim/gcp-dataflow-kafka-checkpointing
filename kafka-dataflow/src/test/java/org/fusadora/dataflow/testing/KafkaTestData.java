package org.fusadora.dataflow.testing;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.io.kafka.KafkaTimestampType;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.fusadora.dataflow.dto.KafkaEventEnvelope;
import org.fusadora.dataflow.dto.TopicConfig;

/**
 * Stateless Kafka-related fixtures used by tests.
 */
public final class KafkaTestData {

    private KafkaTestData() {
    }

    public static KafkaEventEnvelope envelope(String topic, int partition, long offset, String payload) {
        return new KafkaEventEnvelope(topic, partition, offset, payload);
    }

    public static KafkaRecord<String, String> kafkaRecord(String topic, int partition, long offset, String payload) {
        return new KafkaRecord<>(topic, partition, offset, offset,
                KafkaTimestampType.CREATE_TIME, new RecordHeaders(), KV.of("key-" + offset, payload));
    }

    public static TopicConfig topicConfig(String topicName, String datasetName) {
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(topicName);
        topicConfig.setDatasetName(datasetName);
        return topicConfig;
    }

    public static KafkaRecordCoder<String, String> kafkaRecordCoder() {
        return KafkaRecordCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());
    }
}

