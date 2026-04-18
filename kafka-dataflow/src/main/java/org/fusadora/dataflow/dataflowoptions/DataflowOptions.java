package org.fusadora.dataflow.dataflowoptions;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
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

    @Description("Generator ticks per second for KafkaGenerateRandomPipeline. Total records per second per topic equals this value multiplied by randomRecordsPerTick.")
    @Default.Long(100L)
    Long getRandomRecordRatePerSecond();

    void setRandomRecordRatePerSecond(Long randomRecordRatePerSecond);

    @Description("Number of random Kafka records emitted for each generator tick in KafkaGenerateRandomPipeline.")
    @Default.Integer(10)
    Integer getRandomRecordsPerTick();

    void setRandomRecordsPerTick(Integer randomRecordsPerTick);

}
