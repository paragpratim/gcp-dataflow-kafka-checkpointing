package org.fusadora.dataflow.pipelines;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.fusadora.dataflow.dataflowoptions.DataflowOptions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KafkaGenerateRandomPipelineTest {

    @Test
    void buildRandomJsonMessageCreatesExpectedShape() {
        String message = KafkaGenerateRandomPipeline.buildRandomJsonMessage(
                "abc-123",
                "2026-04-18T00:00:00Z",
                42.5d);

        assertEquals("{\"id\":\"abc-123\",\"timestamp\":\"2026-04-18T00:00:00Z\",\"value\":42.500000}", message);
    }

    @Test
    void buildRandomKafkaMessagesCreatesRequestedBatchSize() {
        assertEquals(25, KafkaGenerateRandomPipeline.buildRandomKafkaMessages(25).size());
    }

    @Test
    void resolveRandomRateUsesBeamOptionDefaults() {
        DataflowOptions options = PipelineOptionsFactory.create().as(DataflowOptions.class);

        assertEquals(KafkaGenerateRandomPipeline.DEFAULT_RANDOM_RECORD_RATE_PER_SECOND,
                KafkaGenerateRandomPipeline.resolveRandomRecordRatePerSecond(options));
        assertEquals(KafkaGenerateRandomPipeline.DEFAULT_RANDOM_RECORDS_PER_TICK,
                KafkaGenerateRandomPipeline.resolveRandomRecordsPerTick(options));
    }

    @Test
    void resolveRandomRateRejectsNonPositiveValues() {
        DataflowOptions options = PipelineOptionsFactory.create().as(DataflowOptions.class);
        options.setRandomRecordRatePerSecond(0L);
        options.setRandomRecordsPerTick(-1);

        assertThrows(IllegalArgumentException.class,
                () -> KafkaGenerateRandomPipeline.resolveRandomRecordRatePerSecond(options));
        assertThrows(IllegalArgumentException.class,
                () -> KafkaGenerateRandomPipeline.resolveRandomRecordsPerTick(options));
    }

    @Test
    void buildRandomKafkaMessagesRejectsNonPositiveBatchSizes() {
        assertThrows(IllegalArgumentException.class,
                () -> KafkaGenerateRandomPipeline.buildRandomKafkaMessages(0));
        assertDoesNotThrow(() -> KafkaGenerateRandomPipeline.buildRandomKafkaMessages(1));
    }
}

