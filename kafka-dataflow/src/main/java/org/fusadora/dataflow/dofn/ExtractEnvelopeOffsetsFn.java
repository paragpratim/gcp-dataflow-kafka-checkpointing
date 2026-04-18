package org.fusadora.dataflow.dofn;

import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.fusadora.dataflow.dto.KafkaEventEnvelope;

/**
 * org.fusadora.dataflow.dofn.ExtractEnvelopeOffsetsFn
 * Maps a {@link KafkaEventEnvelope} to a KV of {@code topic:partition} key and offset value.
 * Provides an alternative pre-write offset extraction path from source envelopes.
 * The pipeline uses post-write extraction via {@code ExtractHandledWriteOffsetsFn} as the primary path
 * (reading from the {@code __metadata} RECORD field written to BigQuery). This class is retained as an
 * alternative for scenarios where pre-write extraction is preferred.
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
