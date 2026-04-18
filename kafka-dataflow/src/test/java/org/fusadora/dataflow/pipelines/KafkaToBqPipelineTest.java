package org.fusadora.dataflow.pipelines;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.fusadora.dataflow.dataflowoptions.DataflowOptions;
import org.fusadora.dataflow.di.GuiceInitialiser;
import org.fusadora.dataflow.di.TestDataflowBusinessLogicModule;
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
                Create.of(
                        KafkaTestData.kafkaRecord("test_df", 0, 0L, "a"),
                        KafkaTestData.kafkaRecord("test_df", 0, 1L, "b"))
                        .withCoder(KafkaTestData.kafkaRecordCoder()));
        TestOutputService.setFailingOffsets(Set.of());
        RecordingCheckpointService.seed(Map.of("test_df:0", 0L));

        runPipeline("job-success");

        assertEquals(2L, RecordingCheckpointService.nextOffset("test_df", 0));
        assertEquals(1, TestInputService.bootstrapTopics().size());
        assertEquals("test_df", TestInputService.bootstrapTopics().get(0));
    }

    @Test
    void pipelineStallsCheckpointAtFirstBqWriteFailure() {
        TestInputService.setSourceTransform(
                Create.of(
                        KafkaTestData.kafkaRecord("test_df", 0, 0L, "a"),
                        KafkaTestData.kafkaRecord("test_df", 0, 1L, "b"),
                        KafkaTestData.kafkaRecord("test_df", 0, 2L, "c"))
                        .withCoder(KafkaTestData.kafkaRecordCoder()));
        TestOutputService.setFailingOffsets(Set.of(1L));
        RecordingCheckpointService.seed(Map.of("test_df:0", 0L));

        runPipeline("job-failure-handled");

        // Offset 1 fails BQ write and is not committed; contiguous progression stalls at 1.
        // The failed record will be retried on the next pipeline run.
        assertEquals(1L, RecordingCheckpointService.nextOffset("test_df", 0));
    }

    @Test
    void pipelineAdvancesCheckpointWhenSourceGapTimesOut() {
        TestInputService.setSourceTransform(
                TestStream.create(KafkaTestData.kafkaRecordCoder())
                        .addElements(
                                KafkaTestData.kafkaRecord("test_df", 0, 0L, "a"),
                                KafkaTestData.kafkaRecord("test_df", 0, 2L, "c"))
                        .advanceProcessingTime(Duration.standardMinutes(6))
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
                        .advanceProcessingTime(Duration.standardMinutes(6))
                        .advanceWatermarkToInfinity());
        // Offset 2 fails BQ write; gap-timeout path emits missing offset 1.
        // Only offset 0 (BQ success) and offset 1 (gap-timeout) contribute to the checkpoint.
        TestOutputService.setFailingOffsets(Set.of(2L));
        RecordingCheckpointService.seed(Map.of("test_df:0", 0L));

        runPipeline("job-windowfn-regression");

        // Checkpoint advances through 0 (BQ success) and 1 (gap-timeout) then stalls at 2 (BQ failure).
        assertEquals(2L, RecordingCheckpointService.nextOffset("test_df", 0));
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
