package org.fusadora.dataflow.pipelines;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.fusadora.dataflow.dataflowoptions.DataflowOptions;
import org.fusadora.dataflow.di.DataflowBusinessLogicModule;
import org.fusadora.dataflow.di.GuiceInitialiser;
import org.fusadora.dataflow.utilities.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * org.fusadora.dataflow.pipelines.PipelineRunner
 * Main pipeline runner class.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public class PipelineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineRunner.class);

    public static void main(String[] args) {

        DataflowOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowOptions.class);

        options.setProject(PropertyUtils.getProperty(PropertyUtils.PROJECT_NAME));
        options.setStagingLocation(PropertyUtils.GCS_URL_HEADER
                .concat(PropertyUtils.getProperty(PropertyUtils.BUCKET_DATAFLOW_STAGING))
                .concat(PropertyUtils.GCS_BUCKET_FILE_SEPARATOR)
                .concat("staging"));
        options.setTempLocation(PropertyUtils.GCS_URL_HEADER
                .concat(PropertyUtils.getProperty(PropertyUtils.BUCKET_DATAFLOW_STAGING))
                .concat(PropertyUtils.GCS_BUCKET_FILE_SEPARATOR)
                .concat("temp"));
//        options.setTemplateLocation(PropertyUtils.GCS_URL_HEADER
//                .concat(PropertyUtils.getProperty(PropertyUtils.BUCKET_DATAFLOW_STAGING))
//                .concat(PropertyUtils.GCS_BUCKET_FILE_SEPARATOR)
//                .concat("templates"));

        Pipeline pipeline = Pipeline.create(options);

        BasePipeline pipelineToRun = GuiceInitialiser.getGuiceInitialisedClass(new DataflowBusinessLogicModule(),
                BasePipeline.class, options.getPipelineName());


        LOG.info("Running pipeline : {}", options.getPipelineName());
        pipelineToRun.run(pipeline, options);

    }
}
