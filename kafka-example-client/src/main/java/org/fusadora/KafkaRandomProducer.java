package org.fusadora;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A simple Kafka producer that sends random JSON messages to a specified topic at a defined rate.
 * Messages contain a unique ID, timestamp, and random value.
 *
 * @author Parag Ghosh
 * @since 06/12/2025
 */
public class KafkaRandomProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRandomProducer.class);
    private static final Random rnd = new Random();

    private KafkaRandomProducer() {
        throw new IllegalStateException("Utility class");
    }

    public static void runProducer(Properties kafkaProps, String topic) {
        int ratePerSec = 10; // messages per second

        final Producer<String, String> producer = getKafkaProducer(kafkaProps);
        final AtomicBoolean running = new AtomicBoolean(true);
        final java.util.concurrent.CountDownLatch stopLatch = new java.util.concurrent.CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutdown requested, closing producer...");
            running.set(false);
            stopLatch.countDown();
            try {
                producer.flush();
            } catch (Exception ex) {
                LOGGER.error("Error during producer flush: {}\n", ex.getMessage(), ex);
            }
            producer.close();
        }));

        LOGGER.info("Starting producer -> brokers={} topic={} rate={} msg/s\n", kafkaProps.get("bootstrap.servers"), topic, ratePerSec);

        long intervalMillis = 1000L / ratePerSec;

        final java.util.concurrent.ScheduledExecutorService scheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor();

        final Runnable sendTask = () -> {
            if (!running.get()) return;
            String id = UUID.randomUUID().toString();
            long ts = System.currentTimeMillis();
            int value = rnd.nextInt(10000);

            String message = String.format("{\"id\":\"%s\",\"ts\":%d,\"value\":%d}", id, ts, value);

            ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(topic, id, message);
            producer.send(kafkaRecord, (metadata, exception) -> {
                if (exception != null) {
                    LOGGER.error("Send failed: {}\n", exception.getMessage());
                }
            });
        };

        scheduler.scheduleAtFixedRate(sendTask, 0, intervalMillis, java.util.concurrent.TimeUnit.MILLISECONDS);

        try {
            stopLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private static Producer<String, String> getKafkaProducer(Properties kafkaProps) {
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("client.id", "kafka-random-producer-" + UUID.randomUUID().toString());

        return new KafkaProducer<>(kafkaProps);
    }
}
