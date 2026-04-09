package org.fusadora.dataflow.testing.stubs;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.fusadora.dataflow.services.InputService;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Shared configurable Kafka source stub for DI-driven tests.
 */
public class TestInputService implements InputService, Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final List<String> BOOTSTRAP_TOPICS = new ArrayList<>();
    private static PTransform<PBegin, PCollection<KafkaRecord<String, String>>> sourceTransform;

    public static void reset() {
        BOOTSTRAP_TOPICS.clear();
        sourceTransform = null;
    }

    public static void setSourceTransform(PTransform<PBegin, PCollection<KafkaRecord<String, String>>> transform) {
        sourceTransform = transform;
    }

    public static List<String> bootstrapTopics() {
        return List.copyOf(BOOTSTRAP_TOPICS);
    }

    @Override
    public void bootstrapOffsetsFromCheckpoint(String brokerIp, String topic) {
        BOOTSTRAP_TOPICS.add(topic);
    }

    @Override
    public PCollection<KafkaRecord<String, String>> readFromKafka(Pipeline pipeline, String brokerIp, String topic,
                                                                   String transformName) {
        if (sourceTransform == null) {
            throw new IllegalStateException("TestInputService sourceTransform is not configured");
        }
        return pipeline.apply(transformName, sourceTransform);
    }
}
