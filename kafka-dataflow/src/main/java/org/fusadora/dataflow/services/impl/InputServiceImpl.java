package org.fusadora.dataflow.services.impl;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.fusadora.dataflow.services.InputService;
import org.fusadora.dataflow.utilities.PropertyUtils;
import org.jetbrains.annotations.NotNull;

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

    private static @NotNull Map<String, Object> getKafkaConfigMap() {
        String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';";
        String jaasCfg = String.format(jaasTemplate, PropertyUtils.getProperty(PropertyUtils.KAFKA_SASL_USERNAME)
                , PropertyUtils.getProperty(PropertyUtils.KAFKA_SASL_PASSWORD));
        Map<String, Object> kafkaConsumerConfig = new HashMap<>();
        kafkaConsumerConfig.put("client.id", PropertyUtils.getProperty(PropertyUtils.KAFKA_CONSUMER_CLIENT_ID));
        kafkaConsumerConfig.put("group.id", PropertyUtils.getProperty(PropertyUtils.KAFKA_CONSUMER_GROUP_ID));
        kafkaConsumerConfig.put("auto.offset.reset", "earliest");
        kafkaConsumerConfig.put("enable.auto.commit", "false");
        kafkaConsumerConfig.put("security.protocol", "SASL_SSL");
        kafkaConsumerConfig.put("sasl.jaas.config", jaasCfg);
        kafkaConsumerConfig.put("sasl.mechanism", "PLAIN");
        kafkaConsumerConfig.put("client.dns.lookup", "use_all_dns_ips");
        kafkaConsumerConfig.put("session.timeout.ms", "45000");
        return kafkaConsumerConfig;
    }

    @Override
    public PCollection<KafkaRecord<String, String>> readFromKafka(Pipeline pipeline, String brokerIp, String topic, String transformName) {
        Map<String, Object> kafkaConsumerConfig = getKafkaConfigMap();

        return pipeline.apply(transformName, KafkaIO.<String, String>read()
                .withBootstrapServers(brokerIp.concat(KAFKA_PORT))
                .withTopic(topic)
                .withConsumerConfigUpdates(kafkaConsumerConfig)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .commitOffsetsInFinalize());
    }
}
