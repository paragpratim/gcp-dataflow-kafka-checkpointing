package org.fusadora.dataflow.pipelines;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaGenerateRandomPipelineTest {

    @Test
    void buildRandomJsonMessageCreatesExpectedShape() {
        String message = KafkaGenerateRandomPipeline.buildRandomJsonMessage(
                "abc-123",
                "2026-04-18T00:00:00Z",
                42.5d);

        assertEquals("{\"id\":\"abc-123\",\"timestamp\":\"2026-04-18T00:00:00Z\",\"value\":42.500000}", message);
    }
}

