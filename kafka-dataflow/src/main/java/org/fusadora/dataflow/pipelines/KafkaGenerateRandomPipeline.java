package org.fusadora.dataflow.pipelines;

import com.google.inject.Inject;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.fusadora.dataflow.dataflowoptions.DataflowOptions;
import org.fusadora.dataflow.dto.TopicConfig;
import org.fusadora.dataflow.services.CheckpointService;
import org.fusadora.dataflow.services.InputService;
import org.fusadora.dataflow.services.OutputService;
import org.fusadora.dataflow.utilities.PropertyUtils;
import org.fusadora.dataflow.utilities.TopicConfigLoader;
import org.joda.time.Duration;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * org.fusadora.dataflow.pipelines.KafkaGenerateRandomPipeline
 * This pipeline generates random JSON messages and writes them to specified Kafka topics at a configurable high rate.
 *
 * @author Parag Ghosh
 * @since 18/04/2026
 */
public class KafkaGenerateRandomPipeline extends BasePipeline {

    public static final String PIPELINE_NAME = "KafkaGenerateRandom";
    static final long DEFAULT_RANDOM_RECORD_RATE_PER_SECOND = 100L;
    static final int DEFAULT_RANDOM_RECORDS_PER_TICK = 10;

    @Inject
    public KafkaGenerateRandomPipeline(InputService inputService, OutputService outputService,
                                       CheckpointService checkpointService) {
        super(inputService, outputService, checkpointService);
    }

    /**
     * Builds a JSON message with the given randomId, timestamp, and randomValue.
     *
     * @param randomId    A unique identifier for the message, typically a UUID.
     * @param timestamp   The timestamp when the message is generated, formatted as an ISO 8601 string.
     * @param randomValue A random double value that can be used for testing or simulation purposes.
     * @return A JSON string representing the message, structured as:
     * {
     * "id": "randomId",
     * "timestamp": "timestamp",
     * "value": randomValue
     * }
     */
    static String buildRandomJsonMessage(String randomId, String timestamp, double randomValue) {
        return String.format(Locale.ROOT,
                "{\"id\":\"%s\",\"timestamp\":\"%s\",\"value\":%.6f}",
                randomId, timestamp, randomValue);
    }

    static List<KV<String, String>> buildRandomKafkaMessages(int recordCount) {
        Preconditions.checkArgument(recordCount > 0, "randomRecordsPerTick must be greater than 0");

        List<KV<String, String>> messages = new ArrayList<>(recordCount);
        for (int index = 0; index < recordCount; index++) {
            String randomId = UUID.randomUUID().toString();
            String timestamp = Instant.now().toString();
            double randomValue = ThreadLocalRandom.current().nextDouble();
            messages.add(KV.of(randomId, buildRandomJsonMessage(randomId, timestamp, randomValue)));
        }

        return messages;
    }

    static long resolveRandomRecordRatePerSecond(DataflowOptions pipelineOptions) {
        long randomRecordRatePerSecond = Objects.requireNonNullElse(
                pipelineOptions.getRandomRecordRatePerSecond(),
                DEFAULT_RANDOM_RECORD_RATE_PER_SECOND);
        Preconditions.checkArgument(randomRecordRatePerSecond > 0,
                "randomRecordRatePerSecond must be greater than 0");
        return randomRecordRatePerSecond;
    }

    static int resolveRandomRecordsPerTick(DataflowOptions pipelineOptions) {
        int randomRecordsPerTick = Objects.requireNonNullElse(
                pipelineOptions.getRandomRecordsPerTick(),
                DEFAULT_RANDOM_RECORDS_PER_TICK);
        Preconditions.checkArgument(randomRecordsPerTick > 0,
                "randomRecordsPerTick must be greater than 0");
        return randomRecordsPerTick;
    }

    @Override
    void run(Pipeline pipeline, DataflowOptions pipelineOptions) {
        String brokerHost = PropertyUtils.getProperty(PropertyUtils.KAFKA_BROKER_HOST);
        long randomRecordRatePerSecond = resolveRandomRecordRatePerSecond(pipelineOptions);
        int randomRecordsPerTick = resolveRandomRecordsPerTick(pipelineOptions);

        for (TopicConfig topicConfig : Objects.requireNonNull(TopicConfigLoader.readConfig()).getTopicConfigList()) {
            PCollection<KV<String, String>> randomMessage = pipeline
                    .apply("Generate Random Sequence [" + topicConfig.getTopicName() + "]",
                            GenerateSequence.from(0).withRate(randomRecordRatePerSecond, Duration.standardSeconds(1)))
                    .apply("Build Random Kafka Messages [" + topicConfig.getTopicName() + "]", FlatMapElements
                            .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                            .via(unused -> buildRandomKafkaMessages(randomRecordsPerTick)));

            getOutputService().writeToKafka(
                    randomMessage,
                    "Write Random Messages to Kafka [" + topicConfig.getTopicName() + "]",
                    brokerHost,
                    topicConfig.getTopicName());
        }

        pipeline.run();
    }

}

