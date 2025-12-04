package org.fusadora.dataflow.pipelines;

import com.google.inject.Inject;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.fusadora.dataflow.dataflowoptions.DataflowOptions;
import org.fusadora.dataflow.dto.TopicConfig;
import org.fusadora.dataflow.dto.TopicConfigs;
import org.fusadora.dataflow.ptransform.KafkaToMessageTransform;
import org.fusadora.dataflow.ptransform.WriteRawMessageTransform;
import org.fusadora.dataflow.services.InputService;
import org.fusadora.dataflow.services.OutputService;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * org.fusadora.dataflow.pipelines.KafkaToBqPipeline
 * Pipeline to process Kafka messages to BQ.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public class KafkaToBqPipeline extends BasePipeline {

    public static final String PIPELINE_NAME = "KafkaToBqPipeline";
    private static final Logger LOG = LoggerFactory.getLogger(KafkaToBqPipeline.class);
    private static final long WINDOW_SIZE = 1L;

    @Inject
    public KafkaToBqPipeline(InputService aInputService, OutputService aOutputService) {
        super(aInputService, aOutputService);
    }

    @Override
    void run(Pipeline pipeline, DataflowOptions pipelineOptions) {
        for (TopicConfig topicConfig : Objects.requireNonNull(TopicConfigs.readConfig()).getTopicConfigList()) {

            //Read from Kafka
            PCollection<String> kafkaMessage = pipeline.apply("Read from Kafka [" + topicConfig.getTopicName() + "]"
                            , new KafkaToMessageTransform(getInputService(), topicConfig.getTopicName()))
                    .apply(Window.into(FixedWindows.of(Duration.standardMinutes(WINDOW_SIZE))));


            //Write raw messages to Bigquery without any transformation to preserve read records as-is
            kafkaMessage.apply("Write Raw Messages to Bigquery", new WriteRawMessageTransform(getOutputService(), topicConfig));

        }

        pipeline.run();
    }
}
