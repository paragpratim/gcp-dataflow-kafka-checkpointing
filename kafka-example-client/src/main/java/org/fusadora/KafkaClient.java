package org.fusadora;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

/**
 * org.example.KafkaClient
 * <p>Description: A Kafka client that can act as either a producer or consumer based on command-line arguments.
 * It connects to a Kafka cluster using provided broker addresses, topic name, username, and password.
 *
 * @author Parag Ghosh
 * @since 15/12/2025
 */
public class KafkaClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClient.class);

    public static void main(String[] args) {
        LOGGER.info("KafkaClient stating...");

        String brokers = "";
        String topic = "";
        String username = "";
        String password = "";
        String process = "";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--brokers" -> {
                    if (i + 1 < args.length) brokers = args[++i];
                }
                case "--topic" -> {
                    if (i + 1 < args.length) topic = args[++i];
                }
                case "--username" -> {
                    if (i + 1 < args.length) username = args[++i];
                }
                case "--password" -> {
                    if (i + 1 < args.length) password = args[++i];
                }
                case "--process" -> {
                    if (i + 1 < args.length) process = args[++i];
                }
                case "--help" -> printUsageAndExit();
                default -> throw new IllegalStateException("Unexpected value: " + args[i]);
            }
        }

        Properties kafkaProps = new Properties();
        addGenericConfig(kafkaProps, brokers, username, password);
        if (Objects.equals(process, "consumer")) {
            SimpleConsumer.runConsumer(kafkaProps, topic);
        } else if (Objects.equals(process, "producer")) {
            KafkaRandomProducer.runProducer(kafkaProps, topic);
        } else {
            LOGGER.error("Unknown process type: {}", process);
            printUsageAndExit();
        }
    }

    private static void addGenericConfig(Properties kafkaProps, String brokers, String username, String password) {
        String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';";
        String jaasCfg = String.format(jaasTemplate, username, password);
        kafkaProps.put("bootstrap.servers", brokers);
        // copy the same security/SASL properties you used for the producer (security.protocol, sasl.*)
        kafkaProps.put("security.protocol", "SASL_SSL");
        kafkaProps.put("sasl.jaas.config", jaasCfg);
        kafkaProps.put("sasl.mechanism", "PLAIN");
        kafkaProps.put("client.dns.lookup", "use_all_dns_ips");
        kafkaProps.put("session.timeout.ms", "45000");
    }

    private static void printUsageAndExit() {
        LOGGER.warn("Usage: java -jar KafkaClient.jar [--brokers <broker1,broker2>] [--topic <topic>] [--username <username>] [--password <password>] [--process <consumer|producer>]");
        System.exit(0);
    }
}
