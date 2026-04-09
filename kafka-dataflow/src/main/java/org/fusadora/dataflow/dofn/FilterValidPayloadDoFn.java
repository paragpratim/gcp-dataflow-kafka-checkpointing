package org.fusadora.dataflow.dofn;

import org.apache.beam.sdk.transforms.DoFn;
import org.fusadora.dataflow.dto.KafkaEventEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Filters out {@link KafkaEventEnvelope} records whose payload contains a configured
 * invalid-payload keyword. Keeping this concern separate from the mapping DoFn ensures
 * each class has a single responsibility.
 */
@SuppressWarnings("unused") // Instantiated from pipeline transform wiring
public class FilterValidPayloadDoFn extends DoFn<KafkaEventEnvelope, KafkaEventEnvelope> {

    private static final Logger LOG = LoggerFactory.getLogger(FilterValidPayloadDoFn.class);

    private final String invalidPayloadKeyword;

    /**
     * @param invalidPayloadKeyword envelopes whose payload contains this keyword are dropped.
     */
    public FilterValidPayloadDoFn(String invalidPayloadKeyword) {
        this.invalidPayloadKeyword = Objects.requireNonNull(invalidPayloadKeyword,
                "invalidPayloadKeyword must not be null");
    }

    @SuppressWarnings("unused") // Invoked by Beam runtime via @ProcessElement
    @ProcessElement
    public void processElement(ProcessContext context) {
        KafkaEventEnvelope envelope = Objects.requireNonNull(context.element());
        if (envelope.getPayload() != null && envelope.getPayload().contains(invalidPayloadKeyword)) {
            LOG.warn("Dropping envelope with invalid payload keyword [{}] topic={} partition={} offset={}",
                    invalidPayloadKeyword, envelope.getTopic(), envelope.getPartition(), envelope.getOffset());
            return;
        }
        context.output(envelope);
    }
}

