package org.fusadora.dataflow.di;

import org.fusadora.dataflow.pipelines.BasePipeline;
import org.fusadora.dataflow.pipelines.KafkaGenerateRandomPipeline;
import org.fusadora.dataflow.pipelines.KafkaToBqPipeline;
import org.fusadora.dataflow.services.CheckpointService;
import org.fusadora.dataflow.services.InputService;
import org.fusadora.dataflow.services.OutputService;
import org.fusadora.dataflow.testing.stubs.RecordingCheckpointService;
import org.fusadora.dataflow.testing.stubs.TestInputService;
import org.fusadora.dataflow.testing.stubs.TestOutputService;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DataflowBusinessLogicModuleWiringTest {

    @Test
    void testModuleBindsPipelineAndSwapsServicesWithTestStubs() {
        TestDataflowBusinessLogicModule module = new TestDataflowBusinessLogicModule();

        BasePipeline pipeline = GuiceInitialiser.getGuiceInitialisedClass(
                module, BasePipeline.class, KafkaToBqPipeline.PIPELINE_NAME);
        BasePipeline randomGeneratorPipeline = GuiceInitialiser.getGuiceInitialisedClass(
                module, BasePipeline.class, KafkaGenerateRandomPipeline.PIPELINE_NAME);
        InputService inputService = GuiceInitialiser.getGuiceInitialisedClass(module, InputService.class);
        OutputService outputService = GuiceInitialiser.getGuiceInitialisedClass(module, OutputService.class);
        CheckpointService checkpointService = GuiceInitialiser.getGuiceInitialisedClass(module, CheckpointService.class);

        assertInstanceOf(KafkaToBqPipeline.class, pipeline);
        assertInstanceOf(KafkaGenerateRandomPipeline.class, randomGeneratorPipeline);
        assertEquals(TestInputService.class, inputService.getClass());
        assertEquals(TestOutputService.class, outputService.getClass());
        assertEquals(RecordingCheckpointService.class, checkpointService.getClass());
    }
}
