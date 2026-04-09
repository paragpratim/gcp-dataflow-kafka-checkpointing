package org.fusadora.dataflow.dto;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

import java.io.Serial;
import java.util.Objects;

/**
 * org.fusadora.dataflow.dto.KafkaEventEnvelope
 * DTO to represent a Kafka event with topic, partition, offset and payload.
 * This is used to encapsulate the Kafka event data and metadata for processing in the dataflow pipeline.
 * The partition key is derived from the topic and partition to ensure that events from the same partition are processed together, maintaining order.
 *
 * @author Parag Ghosh
 * @since 10/04/2026
 */
@DefaultCoder(SerializableCoder.class)
public class KafkaEventEnvelope extends BaseDto {

    @Serial
    private static final long serialVersionUID = 1L;

    private String topic;
    private int partition;
    private long offset;
    private String payload;

    public KafkaEventEnvelope() {
        super();
    }

    public KafkaEventEnvelope(String topic, int partition, long offset, String payload) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.payload = payload;
    }

    public String getPartitionKey() {
        return topic + ":" + partition;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaEventEnvelope that)) {
            return false;
        }
        return partition == that.partition && offset == that.offset && Objects.equals(topic, that.topic)
                && Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, offset, payload);
    }
}

