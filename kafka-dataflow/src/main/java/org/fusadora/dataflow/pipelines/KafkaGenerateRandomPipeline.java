package org.fusadora.dataflow.pipelines;

import com.google.inject.Inject;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.MapElements;
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

import java.util.Locale;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * org.fusadora.dataflow.pipelines.KafkaGenerateRandomPipeline
 * This pipeline generates random JSON messages and writes them to specified Kafka topics at a rate of 1 message per second.
 *
 * @author Parag Ghosh
 * @since 18/04/2026
 */
public class KafkaGenerateRandomPipeline extends BasePipeline {

    public static final String PIPELINE_NAME = "KafkaGenerateRandom";

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

    @Override
    void run(Pipeline pipeline, DataflowOptions pipelineOptions) {
        String brokerHost = PropertyUtils.getProperty(PropertyUtils.KAFKA_BROKER_HOST);

        for (TopicConfig topicConfig : Objects.requireNonNull(TopicConfigLoader.readConfig()).getTopicConfigList()) {
            PCollection<KV<String, String>> randomMessage = pipeline
                    .apply("Generate Random Sequence [" + topicConfig.getTopicName() + "]",
                            GenerateSequence.from(0).withRate(1, Duration.standardSeconds(1)))
                    .apply("Build Random Kafka Message [" + topicConfig.getTopicName() + "]", MapElements
                            .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                            .via(unused -> {
                                String randomId = UUID.randomUUID().toString();
                                String timestamp = java.time.Instant.now().toString();
                                double randomValue = ThreadLocalRandom.current().nextDouble();
                                return KV.of(randomId, buildRandomJsonMessage(randomId, timestamp, randomValue));
                            }));

            getOutputService().writeToKafka(
                    randomMessage,
                    "Write Random Messages to Kafka [" + topicConfig.getTopicName() + "]",
                    brokerHost,
                    topicConfig.getTopicName());
        }

        pipeline.run();
    }

}

