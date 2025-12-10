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
        kafkaConsumerConfig.put("client.id", "gcp-dataflow-client-dev");
        kafkaConsumerConfig.put("group.id", "gcp-dataflow-group-dev");
        kafkaConsumerConfig.put("auto.offset.reset", "earliest");
        kafkaConsumerConfig.put("enable.auto.commit", "false");
        kafkaConsumerConfig.put("security.protocol", "SASL_SSL");
        kafkaConsumerConfig.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='D4JH6JHRZV5HATA3' password='cflt5bfS4vOg6/3uIN7eQ/oQH0J0ZSOVz0nypLIJF2e1RYRn4zIjm2LPuUTG0PxA';");
        kafkaConsumerConfig.put("sasl.mechanism", "PLAIN");
        kafkaConsumerConfig.put("client.dns.lookup", "use_all_dns_ips");
        kafkaConsumerConfig.put("session.timeout.ms", "45000");

        return pipeline.apply(transformName, KafkaIO.<String, String>read()
                .withBootstrapServers(brokerIp.concat(KAFKA_PORT))
                .withTopic(topic)
                .withConsumerConfigUpdates(kafkaConsumerConfig)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .commitOffsetsInFinalize());
    }
}
