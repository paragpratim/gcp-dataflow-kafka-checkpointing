package org.fusadora.dataflow.dofn;

import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.fusadora.dataflow.dto.KafkaEventEnvelope;

/**
 * org.fusadora.dataflow.dofn.ExtractEnvelopeOffsetsFn
 * Maps a {@link KafkaEventEnvelope} to a KV of {@code topic:partition} key and offset value.
 * Used to derive handled offsets directly from source envelopes before the BigQuery write step.
 * This avoids relying on BigQuery Storage Write API result rows, which strip unknown schema fields
 * (including Kafka metadata) during proto serialisation, making offset recovery impossible post-write.
 *
 * @author Parag Ghosh
 * @since 04/2026
 */
@SuppressWarnings("unused") // Instantiated from pipeline transform wiring
public class ExtractEnvelopeOffsetsFn extends SimpleFunction<KafkaEventEnvelope, KV<String, Long>> {

    @Override
    public KV<String, Long> apply(KafkaEventEnvelope envelope) {
        return KV.of(envelope.getPartitionKey(), envelope.getOffset());
    }
}
