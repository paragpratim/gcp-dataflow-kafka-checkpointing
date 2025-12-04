package org.fusadora.dataflow.pipelines;

import org.apache.beam.sdk.Pipeline;
import org.fusadora.dataflow.dataflowoptions.DataflowOptions;
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

    protected BasePipeline(InputService aInputService, OutputService aOutputService) {
        inputService = aInputService;
        outputService = aOutputService;
    }

    abstract void run(Pipeline pipeline, DataflowOptions pipelineOptions);

    public InputService getInputService() {
        return inputService;
    }

    public OutputService getOutputService() {
        return outputService;
    }
}
