package org.fusadora.dataflow.pipelines;

import com.google.inject.Inject;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.fusadora.dataflow.dataflowoptions.DataflowOptions;
import org.fusadora.dataflow.dofn.ExtractEnvelopeOffsetsFn;
import org.fusadora.dataflow.dofn.ExtractFailedRowsDoFn;
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

    @Inject
    public KafkaToBqPipeline(InputService aInputService, OutputService aOutputService,
                             CheckpointService checkpointService) {
        super(aInputService, aOutputService, checkpointService);
    }

    @Override
    void run(Pipeline pipeline, DataflowOptions pipelineOptions) {
        final String jobId = pipelineOptions.getJobName();
        final String brokerHost = PropertyUtils.getProperty(PropertyUtils.KAFKA_BROKER_HOST);
        final long gapTimeoutSeconds = parseLongOrDefault(
                PropertyUtils.getProperty(PropertyUtils.OFFSET_GAP_TIMEOUT_SECONDS)
        );
        final boolean gapAuditEnabled = Boolean.parseBoolean(
                PropertyUtils.getProperty(PropertyUtils.OFFSET_GAP_AUDIT_ENABLED));

        for (TopicConfig topicConfig : Objects.requireNonNull(TopicConfigLoader.readConfig()).getTopicConfigList()) {
            //Bootstrap offsets from checkpoint for the topic
            getInputService().bootstrapOffsetsFromCheckpoint(brokerHost, topicConfig.getTopicName());

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

            // Extract handled offsets directly from contiguous envelopes before the BQ write.
            // BQ Storage Write API strips fields not in the table schema from returned rows during
            // proto serialisation, so metadata fields cannot be recovered from write result rows.
            PCollection<KV<String, Long>> handledOffsetsFromBq = contiguousKafkaMessage
                    .apply("Extract Handled Offsets From Envelopes [" + topicConfig.getTopicName() + "]",
                            MapElements.via(new ExtractEnvelopeOffsetsFn()));

            //Write to BigQuery and log any failures for audit.
            WriteResult writeResult = contiguousKafkaMessage.apply("Write Raw Messages to BigQuery",
                    new WriteRawMessageTransform(getOutputService(), topicConfig));
            writeResult.getFailedStorageApiInserts()
                    .apply("Log BQ Storage API Failures [" + topicConfig.getTopicName() + "]",
                            ParDo.of(new ExtractFailedRowsDoFn(topicConfig.getTopicName())));

            // Commit state is key+window scoped, so normalize both inputs to a single global window before merge.
            PCollection<KV<String, Long>> handledOffsetsFromBqGlobal = handledOffsetsFromBq
                    .apply("Global Window Handled BQ Offsets [" + topicConfig.getTopicName() + "]",
                            Window.into(new GlobalWindows()));

            PCollection<KV<String, Long>> sourceGapTimeoutOffsetsGlobal = sourceGapTimeoutOffsets
                    .apply("Global Window Gap Timeout Offsets [" + topicConfig.getTopicName() + "]",
                            Window.into(new GlobalWindows()));

            //Merge source gap-timeout offsets with BQ handled offsets and commit from one stream.
            PCollectionList.of(handledOffsetsFromBqGlobal)
                    .and(sourceGapTimeoutOffsetsGlobal)
                    .apply("Merge All Handled Offsets [" + topicConfig.getTopicName() + "]", Flatten.pCollections())
                    .apply("Commit Offsets From Handled Stream [" + topicConfig.getTopicName() + "]",
                            new CommitHandledOffsetsTransform(getCheckpointService(), jobId));

        }

        pipeline.run();
    }

    private long parseLongOrDefault(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException nfe) {
            LOG.warn("Invalid numeric config value [{}], falling back to {}", value, KafkaToBqPipeline.DEFAULT_GAP_TIMEOUT_SECONDS);
            return KafkaToBqPipeline.DEFAULT_GAP_TIMEOUT_SECONDS;
        }
    }

}
