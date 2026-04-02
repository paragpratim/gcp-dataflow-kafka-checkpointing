package org.fusadora.dataflow.di;

import org.fusadora.dataflow.services.CheckpointService;
import org.fusadora.dataflow.services.InputService;
import org.fusadora.dataflow.services.OutputService;
import org.fusadora.dataflow.testing.stubs.RecordingCheckpointService;
import org.fusadora.dataflow.testing.stubs.TestInputService;
import org.fusadora.dataflow.testing.stubs.TestOutputService;

/**
 * Test DI module that reuses production wiring and swaps service implementations with test stubs.
 */
public class TestDataflowBusinessLogicModule extends DataflowBusinessLogicModule {

    @Override
    protected Class<? extends InputService> getInputService() {
        return TestInputService.class;
    }

    @Override
    protected Class<? extends OutputService> getOutputService() {
        return TestOutputService.class;
    }

    @Override
    protected Class<? extends CheckpointService> getCheckpointService() {
        return RecordingCheckpointService.class;
    }
}
