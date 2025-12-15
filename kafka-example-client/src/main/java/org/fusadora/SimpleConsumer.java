package org.fusadora;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * org.example.SimpleConsumer
 * <p>Description: A simple Kafka consumer that connects to a Kafka cluster, subscribes to a topic,
 * and continuously polls for new messages, printing their keys, values, partitions, and offsets.
 *
 * @author Parag Ghosh
 * @since 06/12/2025
 */
public class SimpleConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumer.class);

    private SimpleConsumer() {
        throw new IllegalStateException("Utility class");
    }

    public static void runConsumer(Properties kafkaProps, String topic) {

        KafkaConsumer<String, String> consumer = getKafkaConsumer(kafkaProps);

        // add shutdown hook to wake the consumer on JVM termination

        try (consumer) {
            consumer.subscribe(Collections.singletonList(topic));
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Shutdown requested, waking consumer");
                consumer.wakeup();
            }));
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> r : records) {
                    LOGGER.info("key=[{}] value=[{}] partition=[{}] offset=[{}]",
                            r.key(), r.value(), r.partition(), r.offset());
                    // parse r.value() as JSON if needed
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            // expected on shutdown
        }
    }

    private static KafkaConsumer<String, String> getKafkaConsumer(Properties kafkaProps) {
        kafkaProps.put("group.id", "simple-consumer-group");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("auto.offset.reset", "earliest");
        // copy the same security/SASL properties you used for the producer (security.protocol, sasl.*)
        kafkaProps.put("acks", "all");

        return new KafkaConsumer<>(kafkaProps);
    }
}
