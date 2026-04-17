package org.fusadora.dataflow.di;

import org.fusadora.dataflow.pipelines.BasePipeline;
import org.fusadora.dataflow.pipelines.KafkaGenerateRandomPipeline;
import org.fusadora.dataflow.pipelines.KafkaToBqPipeline;
import org.fusadora.dataflow.services.CheckpointService;
import org.fusadora.dataflow.services.InputService;
import org.fusadora.dataflow.services.OutputService;
import org.fusadora.dataflow.services.impl.FirestoreCheckpointServiceImpl;
import org.fusadora.dataflow.services.impl.InputServiceImpl;
import org.fusadora.dataflow.services.impl.OutputServiceImpl;

/**
 * org.fusadora.dataflow.di.DataflowBusinessLogicModule
 * Guice DI Modules.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public class DataflowBusinessLogicModule extends CoreBusinessLogicModule {

    @Override
    protected void configure() {
        bindServices();
    }

    protected void bindServices() {
        bind(BasePipeline.class, KafkaToBqPipeline.class, KafkaToBqPipeline.PIPELINE_NAME);
        bind(BasePipeline.class, KafkaGenerateRandomPipeline.class, KafkaGenerateRandomPipeline.PIPELINE_NAME);

        bind(InputService.class, getInputService());
        bind(OutputService.class, getOutputService());
        bind(CheckpointService.class, getCheckpointService());
    }

    protected Class<? extends InputService> getInputService() {
        return InputServiceImpl.class;
    }

    protected Class<? extends OutputService> getOutputService() {
        return OutputServiceImpl.class;
    }

    protected Class<? extends CheckpointService> getCheckpointService() {
        return FirestoreCheckpointServiceImpl.class;
    }
}
