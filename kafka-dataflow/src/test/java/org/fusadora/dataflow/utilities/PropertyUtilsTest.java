package org.fusadora.dataflow.utilities;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PropertyUtilsTest {

    @Test
    void normalizeKafkaBootstrapServersStripsProtocolAndKeepsPort() {
        String bootstrapServers = PropertyUtils.normalizeKafkaBootstrapServers(
                "SASL_SSL://broker-1.confluent.cloud:9092");

        assertEquals("broker-1.confluent.cloud:9092", bootstrapServers);
    }

    @Test
    void normalizeKafkaBootstrapServersSupportsMultipleEndpoints() {
        String bootstrapServers = PropertyUtils.normalizeKafkaBootstrapServers(
                "SASL_SSL://broker-1:9092, SASL_SSL://broker-2:9092/");

        assertEquals("broker-1:9092,broker-2:9092", bootstrapServers);
    }

    @Test
    void resolveKafkaSecurityProtocolUsesUriScheme() {
        String protocol = PropertyUtils.resolveKafkaSecurityProtocol("SASL_SSL://broker-1:9092");

        assertEquals("SASL_SSL", protocol);
    }

    @Test
    void resolveKafkaSecurityProtocolFallsBackToDefaultWhenNoSchemeProvided() {
        String protocol = PropertyUtils.resolveKafkaSecurityProtocol("broker-1:9092");

        assertEquals("SASL_SSL", protocol);
    }
}

