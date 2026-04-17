package org.fusadora.dataflow.services.impl;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringSerializer;
import org.fusadora.dataflow.services.OutputService;
import org.fusadora.dataflow.utilities.PropertyUtils;
import org.joda.time.Duration;

import java.util.HashMap;
import java.util.Map;

/**
 * org.fusadora.dataflow.services.impl.OutputServiceImpl
 * Output Service Implementations.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public class OutputServiceImpl implements OutputService {
    @Override
    public WriteResult writeToBqFileLoad(PCollection<TableRow> input, String transformName, String bqTableName,
                                         TableSchema bqTableSchema, String partitionType) {
        return input.apply(transformName,
                BigQueryIO.writeTableRows().to(bqTableName).withSchema(bqTableSchema)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                        .withPropagateSuccessfulStorageApiWrites(true)
                        .ignoreUnknownValues()
                        .withTriggeringFrequency(Duration.standardMinutes(1))
                        .withTimePartitioning(new TimePartitioning().setType(partitionType)));
    }

    @Override
    public void writeToKafka(PCollection<KV<String, String>> input, String transformName, String brokerHost, String topicName) {
        String bootstrapServers = PropertyUtils.normalizeKafkaBootstrapServers(brokerHost);
        input.apply(transformName, KafkaIO.<String, String>write()
                .withBootstrapServers(bootstrapServers)
                .withTopic(topicName)
                .withProducerConfigUpdates(getKafkaProducerConfigMap(brokerHost))
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(StringSerializer.class));
    }

    private static Map<String, Object> getKafkaProducerConfigMap(String brokerHost) {
        String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';";
        String jaasCfg = String.format(jaasTemplate, PropertyUtils.getProperty(PropertyUtils.KAFKA_SASL_USERNAME)
                , PropertyUtils.getProperty(PropertyUtils.KAFKA_SASL_PASSWORD));
        Map<String, Object> kafkaProducerConfig = new HashMap<>();
        kafkaProducerConfig.put("client.id", PropertyUtils.getProperty(PropertyUtils.KAFKA_CONSUMER_CLIENT_ID));
        kafkaProducerConfig.put("security.protocol", PropertyUtils.resolveKafkaSecurityProtocol(brokerHost));
        kafkaProducerConfig.put("sasl.jaas.config", jaasCfg);
        kafkaProducerConfig.put("sasl.mechanism", "PLAIN");
        kafkaProducerConfig.put("client.dns.lookup", "use_all_dns_ips");
        kafkaProducerConfig.put("acks", "all");
        return kafkaProducerConfig;
    }
}
