package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * org.example.SimpleConsumer
 * <p>Description: TODO.</p>
 *
 * @author Parag Ghosh
 * @since 06/12/2025
 */
public class SimpleConsumer {
    public static void main(String[] args) {
        String brokers = "pkc-ewzgj.europe-west4.gcp.confluent.cloud:9092";
        String topic = "test_df";

        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", "my-group-id");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        // copy the same security/SASL properties you used for the producer (security.protocol, sasl.*)
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='D4JH6JHRZV5HATA3' password='cflt5bfS4vOg6/3uIN7eQ/oQH0J0ZSOVz0nypLIJF2e1RYRn4zIjm2LPuUTG0PxA';");
        props.put("sasl.mechanism", "PLAIN");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", "45000");
        props.put("acks", "all");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> r : records) {
                    System.out.printf("key=%s value=%s partition=%d offset=%d%n",
                            r.key(), r.value(), r.partition(), r.offset());
                    // parse r.value() as JSON if needed
                }
            }
        } finally {
            consumer.close();
        }
    }
}
