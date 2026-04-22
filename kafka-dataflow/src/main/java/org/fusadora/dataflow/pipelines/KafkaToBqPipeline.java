package org.fusadora.dataflow.pipelines;

import com.google.api.services.bigquery.model.TableRow;
import com.google.inject.Inject;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.fusadora.dataflow.dataflowoptions.DataflowOptions;
import org.fusadora.dataflow.dofn.DropInvalidHandledRowsFn;
import org.fusadora.dataflow.dofn.ExtractFailedRowsDoFn;
import org.fusadora.dataflow.dofn.ExtractHandledWriteOffsetsFn;
import org.fusadora.dataflow.dto.KafkaEventEnvelope;
import org.fusadora.dataflow.dto.TopicConfig;
import org.fusadora.dataflow.ptransform.CommitHandledOffsetsTransform;
import org.fusadora.dataflow.ptransform.KafkaToMessageTransform;
import org.fusadora.dataflow.ptransform.SelectContiguousOffsetsWithGapEventsTransform;
import org.fusadora.dataflow.ptransform.WriteRawMessageTransform;
import org.fusadora.dataflow.services.CheckpointService;
import org.fusadora.dataflow.services.InputService;
import org.fusadora.dataflow.services.OutputService;
import org.fusadora.dataflow.utilities.PropertyUtils;
import org.fusadora.dataflow.utilities.TopicConfigLoader;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * org.fusadora.dataflow.pipelines.KafkaToBqPipeline
 * Pipeline to process Kafka messages to BQ.
 * The pipeline reads from Kafka, identifies contiguous offsets for processing, applies a fixed window, writes to BigQuery,
 * and captures handled offsets for checkpointing.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public class KafkaToBqPipeline extends BasePipeline {

    public static final String PIPELINE_NAME = "KafkaToBqPipeline";
    private static final Logger LOG = LoggerFactory.getLogger(KafkaToBqPipeline.class);
    private static final long WINDOW_SIZE_SECONDS = 10L;
    private static final long DEFAULT_GAP_TIMEOUT_SECONDS = 300L;
    private static final long DEFAULT_CHECKPOINT_COMMIT_INTERVAL_SECONDS = 60L;

    @Inject
    public KafkaToBqPipeline(InputService aInputService, OutputService aOutputService,
                             CheckpointService checkpointService) {
        super(aInputService, aOutputService, checkpointService);
    }

    static long resolveCheckpointCommitIntervalSeconds(TopicConfig topicConfig, long defaultIntervalSeconds) {
        if (topicConfig == null) {
            return defaultIntervalSeconds;
        }
        Long topicCommitIntervalSeconds = topicConfig.getCheckpointCommitIntervalSeconds();
        if (topicCommitIntervalSeconds == null || topicCommitIntervalSeconds <= 0) {
            return defaultIntervalSeconds;
        }
        return topicCommitIntervalSeconds;
    }

    @Override
    void run(Pipeline pipeline, DataflowOptions pipelineOptions) {
        final String jobId = pipelineOptions.getJobName();
        final String brokerHost = PropertyUtils.getProperty(PropertyUtils.KAFKA_BROKER_HOST);
        final long gapTimeoutSeconds = parseLongOrDefault(
                PropertyUtils.getProperty(PropertyUtils.OFFSET_GAP_TIMEOUT_SECONDS),
                DEFAULT_GAP_TIMEOUT_SECONDS,
                PropertyUtils.OFFSET_GAP_TIMEOUT_SECONDS
        );
        final long checkpointCommitIntervalSeconds = parseLongOrDefault(
                PropertyUtils.getProperty(PropertyUtils.CHECKPOINT_COMMIT_INTERVAL_SECONDS),
                DEFAULT_CHECKPOINT_COMMIT_INTERVAL_SECONDS,
                PropertyUtils.CHECKPOINT_COMMIT_INTERVAL_SECONDS
        );
        final boolean gapAuditEnabled = Boolean.parseBoolean(
                PropertyUtils.getProperty(PropertyUtils.OFFSET_GAP_AUDIT_ENABLED));

        for (TopicConfig topicConfig : Objects.requireNonNull(TopicConfigLoader.readConfig()).getTopicConfigList()) {
            final long topicCheckpointCommitIntervalSeconds = resolveCheckpointCommitIntervalSeconds(
                    topicConfig, checkpointCommitIntervalSeconds);

            //Bootstrap offsets from checkpoint for the topic
            getInputService().bootstrapOffsetsFromCheckpoint(brokerHost, Objects.requireNonNull(topicConfig).getTopicName());

            //Read from Kafka and convert to KafkaEventEnvelope
            PCollection<KafkaEventEnvelope> kafkaMessage = pipeline.apply("Read from Kafka [" + topicConfig.getTopicName() + "]"
                    , new KafkaToMessageTransform(getInputService(), topicConfig.getTopicName(), brokerHost));

            //Calculate contiguous kafka offsets and capture timeout-skip offsets for handled checkpointing.
            PCollectionTuple contiguousWithGapEvents = kafkaMessage
                    .apply("Select Contiguous Offsets [" + topicConfig.getTopicName() + "]",
                            new SelectContiguousOffsetsWithGapEventsTransform(
                                    getCheckpointService(), Duration.standardSeconds(gapTimeoutSeconds), gapAuditEnabled));

            //Fixed window of contiguous records
            PCollection<KafkaEventEnvelope> contiguousKafkaMessage = contiguousWithGapEvents
                    .get(SelectContiguousOffsetsWithGapEventsTransform.CONTIGUOUS_MAIN_TAG)
                    .apply("Window Contiguous Offsets [" + topicConfig.getTopicName() + "]",
                            Window.into(FixedWindows.of(Duration.standardSeconds(WINDOW_SIZE_SECONDS))));

            //Fixed window of gap-timeout offsets to be skipped with checkpoint progression.
            PCollection<KV<String, Long>> sourceGapTimeoutOffsets = contiguousWithGapEvents
                    .get(SelectContiguousOffsetsWithGapEventsTransform.GAP_TIMEOUT_OFFSET_TAG)
                    .apply("Window Gap Timeout Offsets [" + topicConfig.getTopicName() + "]",
                            Window.into(FixedWindows.of(Duration.standardSeconds(WINDOW_SIZE_SECONDS))));

            //Write to BigQuery
            WriteResult writeResult = contiguousKafkaMessage.apply("Write Raw Messages to BigQuery",
                    new WriteRawMessageTransform(getOutputService(), topicConfig));

            //Success rows are handled offsets for checkpoint progression.
            PCollection<TableRow> successRows = writeResult.getSuccessfulStorageApiInserts();

            //Failure rows are also treated as handled offsets once captured here with explicit audit logging.
            PCollection<TableRow> failedRows = writeResult.getFailedStorageApiInserts()
                    .apply("Extract and Log BQ Storage API Failures [" + topicConfig.getTopicName() + "]",
                            ParDo.of(new ExtractFailedRowsDoFn(topicConfig.getTopicName())));

            //Convert handled BQ rows into handled offsets.
            PCollection<KV<String, Long>> handledOffsetsFromBq = PCollectionList.of(successRows)
                    .and(failedRows)
                    .apply("Merge Handled BQ Results [" + topicConfig.getTopicName() + "]", Flatten.pCollections())
                    .apply("Extract handled write offsets [" + topicConfig.getTopicName() + "]",
                            MapElements.via(new ExtractHandledWriteOffsetsFn()))
                    .apply("Drop invalid handled rows [" + topicConfig.getTopicName() + "]",
                            ParDo.of(new DropInvalidHandledRowsFn()));

            // Commit state is key+window scoped, so normalize both inputs to a single global window before merge.
            // A repeating processing-time trigger is used so panes fire on a schedule rather than waiting for
            // end-of-time watermark — this enables clean drain and reduces Firestore write frequency.
            Window<KV<String, Long>> globalCommitWindow = Window
                    .<KV<String, Long>>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(
                            AfterProcessingTime.pastFirstElementInPane()
                                    .plusDelayOf(Duration.standardSeconds(WINDOW_SIZE_SECONDS))))
                    .withAllowedLateness(Duration.ZERO)
                    .discardingFiredPanes();

            PCollection<KV<String, Long>> handledOffsetsFromBqGlobal = handledOffsetsFromBq
                    .apply("Global Window Handled BQ Offsets [" + topicConfig.getTopicName() + "]",
                            globalCommitWindow);

            PCollection<KV<String, Long>> sourceGapTimeoutOffsetsGlobal = sourceGapTimeoutOffsets
                    .apply("Global Window Gap Timeout Offsets [" + topicConfig.getTopicName() + "]",
                            globalCommitWindow);

            //Merge source gap-timeout offsets with BQ handled offsets and commit from one stream.
            PCollectionList.of(handledOffsetsFromBqGlobal)
                    .and(sourceGapTimeoutOffsetsGlobal)
                    .apply("Merge All Handled Offsets [" + topicConfig.getTopicName() + "]", Flatten.pCollections())
                    .apply("Commit Offsets From Handled Stream [" + topicConfig.getTopicName() + "]",
                            new CommitHandledOffsetsTransform(getCheckpointService(), jobId,
                                    topicCheckpointCommitIntervalSeconds));

        }

        pipeline.run();
    }

    private long parseLongOrDefault(String value, long defaultValue, String propertyName) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException nfe) {
            LOG.warn("Invalid numeric config for [{}] value [{}], falling back to {}",
                    propertyName, value, defaultValue);
            return defaultValue;
        }
    }

}
