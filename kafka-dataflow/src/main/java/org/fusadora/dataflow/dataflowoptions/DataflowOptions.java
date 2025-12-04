package org.fusadora.dataflow.dataflowoptions;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * org.fusadora.dataflow.dataflowoptions.DataflowOptions
 * Dataflow options
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public interface DataflowOptions extends DataflowPipelineOptions {

    @Description("Pipeline Name")
    @Validation.Required
    String getPipelineName();

    void setPipelineName(String pipelineName);

}
