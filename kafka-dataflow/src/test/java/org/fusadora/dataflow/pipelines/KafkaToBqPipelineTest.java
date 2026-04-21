package org.fusadora.dataflow.pipelines;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestStream;
import org.fusadora.dataflow.dataflowoptions.DataflowOptions;
import org.fusadora.dataflow.di.GuiceInitialiser;
import org.fusadora.dataflow.di.TestDataflowBusinessLogicModule;
import org.fusadora.dataflow.dto.TopicConfig;
import org.fusadora.dataflow.testing.KafkaTestData;
import org.fusadora.dataflow.testing.stubs.RecordingCheckpointService;
import org.fusadora.dataflow.testing.stubs.TestInputService;
import org.fusadora.dataflow.testing.stubs.TestOutputService;
import org.joda.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaToBqPipelineTest {

    @BeforeEach
    void resetTestStores() {
        RecordingCheckpointService.reset();
        TestInputService.reset();
        TestOutputService.reset();
    }

    @Test
    void pipelineCommitsSuccessfulRowsAndBootstrapsTopic() {
        TestInputService.setSourceTransform(
                TestStream.create(KafkaTestData.kafkaRecordCoder())
                        .addElements(
                                KafkaTestData.kafkaRecord("test_df", 0, 0L, "a"),
                                KafkaTestData.kafkaRecord("test_df", 0, 1L, "b"))
                        .advanceProcessingTime(Duration.standardMinutes(2))
                        .advanceWatermarkToInfinity());
        TestOutputService.setFailingOffsets(Set.of());
        RecordingCheckpointService.seed(Map.of("test_df:0", 0L));

        runPipeline("job-success");

        assertEquals(2L, RecordingCheckpointService.nextOffset("test_df", 0));
        assertEquals(1, TestInputService.bootstrapTopics().size());
        assertEquals("test_df", TestInputService.bootstrapTopics().get(0));
    }

    @Test
    void pipelineCommitsAcrossHandledFailureRows() {
        TestInputService.setSourceTransform(
                TestStream.create(KafkaTestData.kafkaRecordCoder())
                        .addElements(
                                KafkaTestData.kafkaRecord("test_df", 0, 0L, "a"),
                                KafkaTestData.kafkaRecord("test_df", 0, 1L, "b"),
                                KafkaTestData.kafkaRecord("test_df", 0, 2L, "c"))
                        .advanceProcessingTime(Duration.standardMinutes(2))
                        .advanceWatermarkToInfinity());
        TestOutputService.setFailingOffsets(Set.of(1L));
        RecordingCheckpointService.seed(Map.of("test_df:0", 0L));

        runPipeline("job-failure-handled");

        assertEquals(3L, RecordingCheckpointService.nextOffset("test_df", 0));
    }

    @Test
    void pipelineAdvancesCheckpointWhenSourceGapTimesOut() {
        TestInputService.setSourceTransform(
                TestStream.create(KafkaTestData.kafkaRecordCoder())
                        .addElements(
                                KafkaTestData.kafkaRecord("test_df", 0, 0L, "a"),
                                KafkaTestData.kafkaRecord("test_df", 0, 2L, "c"))
                        .advanceProcessingTime(Duration.standardMinutes(7))
                        .advanceWatermarkToInfinity());
        TestOutputService.setFailingOffsets(Set.of());
        RecordingCheckpointService.seed(Map.of("test_df:0", 0L));

        runPipeline("job-gap-timeout");

        assertEquals(3L, RecordingCheckpointService.nextOffset("test_df", 0));
    }

    @Test
    void pipelineMergesHandledOffsetsAcrossBranchesWithoutWindowMismatch() {
        TestInputService.setSourceTransform(
                TestStream.create(KafkaTestData.kafkaRecordCoder())
                        .addElements(
                                KafkaTestData.kafkaRecord("test_df", 0, 0L, "a"),
                                KafkaTestData.kafkaRecord("test_df", 0, 2L, "c"))
                        .advanceProcessingTime(Duration.standardMinutes(7))
                        .advanceWatermarkToInfinity());
        // Offset 2 is treated as handled via failed-write path, while gap-timeout path emits missing offset 1.
        TestOutputService.setFailingOffsets(Set.of(2L));
        RecordingCheckpointService.seed(Map.of("test_df:0", 0L));

        runPipeline("job-windowfn-regression");

        assertEquals(3L, RecordingCheckpointService.nextOffset("test_df", 0));
    }

    @Test
    void topicSpecificCheckpointCommitIntervalOverridesGlobalDefault() {
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setCheckpointCommitIntervalSeconds(15L);

        assertEquals(15L, KafkaToBqPipeline.resolveCheckpointCommitIntervalSeconds(topicConfig, 60L));
    }

    @Test
    void checkpointCommitIntervalFallsBackToGlobalDefaultWhenTopicOverrideMissingOrInvalid() {
        TopicConfig missingOverride = new TopicConfig();
        TopicConfig invalidOverride = new TopicConfig();
        invalidOverride.setCheckpointCommitIntervalSeconds(0L);

        assertEquals(60L, KafkaToBqPipeline.resolveCheckpointCommitIntervalSeconds(missingOverride, 60L));
        assertEquals(60L, KafkaToBqPipeline.resolveCheckpointCommitIntervalSeconds(invalidOverride, 60L));
        assertEquals(60L, KafkaToBqPipeline.resolveCheckpointCommitIntervalSeconds(null, 60L));
    }

    private void runPipeline(String jobName) {
        DataflowOptions options = PipelineOptionsFactory.create().as(DataflowOptions.class);
        options.setRunner(DirectRunner.class);
        options.setJobName(jobName);
        options.setPipelineName(KafkaToBqPipeline.PIPELINE_NAME);

        Pipeline pipeline = Pipeline.create(options);
        BasePipeline pipelineToRun = GuiceInitialiser.getGuiceInitialisedClass(
                new TestDataflowBusinessLogicModule(), BasePipeline.class, KafkaToBqPipeline.PIPELINE_NAME);

        pipelineToRun.run(pipeline, options);
    }
}
