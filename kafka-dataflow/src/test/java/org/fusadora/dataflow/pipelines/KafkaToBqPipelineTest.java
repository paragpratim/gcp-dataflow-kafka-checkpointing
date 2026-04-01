package org.fusadora.dataflow.pipelines;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.fusadora.dataflow.dataflowoptions.DataflowOptions;
import org.fusadora.dataflow.testing.BeamTestSupport;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class KafkaToBqPipelineTest {

    @Before
    public void resetCheckpointStore() {
        BeamTestSupport.RecordingCheckpointService.reset();
    }

    @Test
    public void pipelineCommitsSuccessfulRowsAndBootstrapsTopic() {
        BeamTestSupport.FakeInputService inputService = new BeamTestSupport.FakeInputService(
                Create.of(
                        BeamTestSupport.kafkaRecord("test_df", 0, 0L, "a"),
                        BeamTestSupport.kafkaRecord("test_df", 0, 1L, "b"))
                        .withCoder(BeamTestSupport.kafkaRecordCoder()));
        BeamTestSupport.FakeOutputService outputService = new BeamTestSupport.FakeOutputService(Set.of());
        BeamTestSupport.RecordingCheckpointService checkpointService =
                new BeamTestSupport.RecordingCheckpointService(Map.of("test_df:0", 0L));

        runPipeline(inputService, outputService, checkpointService, "job-success");

        assertEquals(2L, BeamTestSupport.RecordingCheckpointService.nextOffset("test_df", 0));
        assertEquals(1, inputService.getBootstrapTopics().size());
        assertEquals("test_df", inputService.getBootstrapTopics().get(0));
    }

    @Test
    public void pipelineCommitsAcrossHandledFailureRows() {
        BeamTestSupport.FakeInputService inputService = new BeamTestSupport.FakeInputService(
                Create.of(
                        BeamTestSupport.kafkaRecord("test_df", 0, 0L, "a"),
                        BeamTestSupport.kafkaRecord("test_df", 0, 1L, "b"),
                        BeamTestSupport.kafkaRecord("test_df", 0, 2L, "c"))
                        .withCoder(BeamTestSupport.kafkaRecordCoder()));
        BeamTestSupport.FakeOutputService outputService = new BeamTestSupport.FakeOutputService(Set.of(1L));
        BeamTestSupport.RecordingCheckpointService checkpointService =
                new BeamTestSupport.RecordingCheckpointService(Map.of("test_df:0", 0L));

        runPipeline(inputService, outputService, checkpointService, "job-failure-handled");

        assertEquals(3L, BeamTestSupport.RecordingCheckpointService.nextOffset("test_df", 0));
    }

    @Test
    public void pipelineAdvancesCheckpointWhenSourceGapTimesOut() {
        BeamTestSupport.FakeInputService inputService = new BeamTestSupport.FakeInputService(
                TestStream.create(BeamTestSupport.kafkaRecordCoder())
                        .addElements(
                                BeamTestSupport.kafkaRecord("test_df", 0, 0L, "a"),
                                BeamTestSupport.kafkaRecord("test_df", 0, 2L, "c"))
                        .advanceProcessingTime(Duration.standardMinutes(6))
                        .advanceWatermarkToInfinity());
        BeamTestSupport.FakeOutputService outputService = new BeamTestSupport.FakeOutputService(Set.of());
        BeamTestSupport.RecordingCheckpointService checkpointService =
                new BeamTestSupport.RecordingCheckpointService(Map.of("test_df:0", 0L));

        runPipeline(inputService, outputService, checkpointService, "job-gap-timeout");

        assertEquals(3L, BeamTestSupport.RecordingCheckpointService.nextOffset("test_df", 0));
    }

    private void runPipeline(BeamTestSupport.FakeInputService inputService,
                             BeamTestSupport.FakeOutputService outputService,
                             BeamTestSupport.RecordingCheckpointService checkpointService,
                             String jobName) {
        DataflowOptions options = PipelineOptionsFactory.create().as(DataflowOptions.class);
        options.setRunner(DirectRunner.class);
        options.setJobName(jobName);
        options.setPipelineName(jobName);

        Pipeline pipeline = Pipeline.create(options);
        new KafkaToBqPipeline(inputService, outputService, checkpointService).run(pipeline, options);
    }
}

