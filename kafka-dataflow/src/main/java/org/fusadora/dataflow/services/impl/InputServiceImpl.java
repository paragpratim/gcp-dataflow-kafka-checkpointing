package org.fusadora.dataflow.services.impl;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.fusadora.dataflow.services.InputService;

import java.util.HashMap;
import java.util.Map;

/**
 * org.fusadora.dataflow.services.impl.InputServiceImpl
 * Input Service Implementations.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public class InputServiceImpl implements InputService {
    private static final String KAFKA_PORT = ":9092";

    @Override
    public PCollection<KafkaRecord<String, String>> readFromKafka(Pipeline pipeline, String brokerIp, String topic, String transformName) {
        Map<String, Object> kafkaConsumerConfig = new HashMap<>();
        kafkaConsumerConfig.put("client.id", "gcp-dataflow");
        kafkaConsumerConfig.put("auto.offset.reset", "earliest");

        return pipeline.apply(transformName, KafkaIO.<String, String>read()
                .withBootstrapServers(brokerIp.concat(KAFKA_PORT))
                .withTopic(topic)
                .withConsumerConfigUpdates(kafkaConsumerConfig)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class));
    }
}
