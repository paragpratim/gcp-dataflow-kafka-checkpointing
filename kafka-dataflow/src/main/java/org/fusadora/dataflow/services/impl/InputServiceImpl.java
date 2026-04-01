package org.fusadora.dataflow.services.impl;

import com.google.inject.Inject;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.fusadora.dataflow.services.CheckpointService;
import org.fusadora.dataflow.services.InputService;
import org.fusadora.dataflow.utilities.PropertyUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
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
    private static final Logger LOG = LoggerFactory.getLogger(InputServiceImpl.class);

    private final CheckpointService checkpointService;

    @Inject
    public InputServiceImpl(CheckpointService checkpointService) {
        this.checkpointService = checkpointService;
    }

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
    public void bootstrapOffsetsFromCheckpoint(String brokerIp, String topic) {
        if (!Boolean.parseBoolean(PropertyUtils.getProperty(PropertyUtils.CHECKPOINT_BOOTSTRAP_ENABLED))) {
            LOG.info("Checkpoint bootstrap is disabled; skipping Kafka offset bootstrap for topic {}", topic);
            return;
        }

        Map<Integer, Long> offsetsByPartition = checkpointService.getTopicPartitionOffsets(topic);
        if (offsetsByPartition.isEmpty()) {
            LOG.info("No checkpoint offsets found for topic {}; skipping bootstrap", topic);
            return;
        }

        Map<String, Object> kafkaConsumerConfig = new HashMap<>(getKafkaConfigMap());
        kafkaConsumerConfig.put("bootstrap.servers", brokerIp.concat(KAFKA_PORT));
        kafkaConsumerConfig.put("key.deserializer", StringDeserializer.class.getName());
        kafkaConsumerConfig.put("value.deserializer", StringDeserializer.class.getName());

        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (Map.Entry<Integer, Long> entry : offsetsByPartition.entrySet()) {
            offsetsToCommit.put(new TopicPartition(topic, entry.getKey()), new OffsetAndMetadata(entry.getValue()));
        }

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConsumerConfig)) {
            Collection<TopicPartition> partitions = offsetsToCommit.keySet();
            consumer.assign(partitions);
            consumer.poll(Duration.ZERO);
            consumer.commitSync(offsetsToCommit);
            LOG.info("Bootstrapped {} partition offsets for topic {}", offsetsToCommit.size(), topic);
        }
    }

    @Override
    public PCollection<KafkaRecord<String, String>> readFromKafka(Pipeline pipeline, String brokerIp, String topic, String transformName) {
        Map<String, Object> kafkaConsumerConfig = getKafkaConfigMap();

        return pipeline.apply(transformName, KafkaIO.<String, String>read()
                .withBootstrapServers(brokerIp.concat(KAFKA_PORT))
                .withTopic(topic)
                .withConsumerConfigUpdates(kafkaConsumerConfig)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class));
    }
}
