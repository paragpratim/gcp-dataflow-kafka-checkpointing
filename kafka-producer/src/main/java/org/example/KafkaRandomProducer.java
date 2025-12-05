package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaRandomProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRandomProducer.class);

    public static void main(String[] args) throws Exception {
        String brokers = "localhost:9092";
        String topic = "test-topic";
        int ratePerSec = 10; // messages per second

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--brokers":
                    if (i + 1 < args.length) brokers = args[++i];
                    break;
                case "--topic":
                    if (i + 1 < args.length) topic = args[++i];
                    break;
                case "--rate":
                    if (i + 1 < args.length) ratePerSec = Integer.parseInt(args[++i]);
                    break;
                case "--help":
                    printUsageAndExit();
                    break;
            }
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "1");

        final Producer<String, String> producer = new KafkaProducer<>(props);
        final AtomicBoolean running = new AtomicBoolean(true);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutdown requested, closing producer...");
            running.set(false);
            try {
                producer.flush();
            } catch (Exception ignored) {
            }
            producer.close();
        }));

        Random rnd = new Random();

        LOGGER.info("Starting producer -> brokers={} topic={} rate={} msg/s\n", brokers, topic, ratePerSec);

        long intervalMillis = Math.max(1, 1000L / Math.max(1, ratePerSec));

        while (running.get()) {
            String id = UUID.randomUUID().toString();
            long ts = System.currentTimeMillis();
            int value = rnd.nextInt(10000);

            String message = String.format("{\"id\":\"%s\",\"ts\":%d,\"value\":%d}", id, ts, value);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, id, message);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOGGER.error("Send failed: {}\n", exception.getMessage());
                }
            });

            try {
                Thread.sleep(intervalMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // final flush/close in case shutdown hook didn't run
        try {
            producer.flush();
            producer.close();
        } catch (Exception ex) {
            LOGGER.error("Error during producer close: {}\n", ex.getMessage());
        }
        LOGGER.info("Producer stopped.");
    }

    private static void printUsageAndExit() {
        LOGGER.info("Usage: java -jar kafka-random-producer.jar [--brokers <host:port>] [--topic <topic>] [--rate <msgs/sec>]");
        System.exit(0);
    }
}
