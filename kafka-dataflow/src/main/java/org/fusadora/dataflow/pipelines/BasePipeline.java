package org.fusadora.dataflow.pipelines;

import org.apache.beam.sdk.Pipeline;
import org.fusadora.dataflow.dataflowoptions.DataflowOptions;
import org.fusadora.dataflow.services.CheckpointService;
import org.fusadora.dataflow.services.InputService;
import org.fusadora.dataflow.services.OutputService;

/**
 * org.fusadora.dataflow.pipelines.BasePipeline
 * Base pipeline implementation. All Pipelines should extend this abstract.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public abstract class BasePipeline {

    private final InputService inputService;
    private final OutputService outputService;
    private final CheckpointService checkpointService;

    protected BasePipeline(InputService aInputService, OutputService aOutputService, CheckpointService aCheckpointService) {
        inputService = aInputService;
        outputService = aOutputService;
        checkpointService = aCheckpointService;
    }

    abstract void run(Pipeline pipeline, DataflowOptions pipelineOptions);

    public InputService getInputService() {
        return inputService;
    }

    public OutputService getOutputService() {
        return outputService;
    }

    public CheckpointService getCheckpointService() {
        return checkpointService;
    }
}
