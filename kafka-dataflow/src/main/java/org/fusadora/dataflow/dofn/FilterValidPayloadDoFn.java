package org.fusadora.dataflow.dofn;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.fusadora.dataflow.dto.KafkaEventEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * org.fusadora.dataflow.dofn.FilterValidPayloadDoFn
 * This is a Beam DoFn that filters out KafkaEventEnvelope records based on the presence of a specific keyword in their payload.
 * If the payload contains the configured invalid-payload keyword, the record is dropped and a warning is logged.
 * Otherwise, the record is passed through to the next stage of the pipeline.
 *
 * @author Parag Ghosh
 * @since 10/04/2026
 */
@SuppressWarnings("unused") // Instantiated from pipeline transform wiring
public class FilterValidPayloadDoFn extends DoFn<KafkaEventEnvelope, KafkaEventEnvelope> {

    private static final Logger LOG = LoggerFactory.getLogger(FilterValidPayloadDoFn.class);

    private final String invalidPayloadKeyword;
    private final TupleTag<KV<String, Long>> droppedOffsetTag;

    /**
     * @param invalidPayloadKeyword envelopes whose payload contains this keyword are dropped.
     */
    public FilterValidPayloadDoFn(String invalidPayloadKeyword) {
        this(invalidPayloadKeyword, null);
    }

    /**
     * @param invalidPayloadKeyword envelopes whose payload contains this keyword are dropped.
     * @param droppedOffsetTag      optional side output tag for dropped offsets, emitted as topic:partition -> offset.
     */
    public FilterValidPayloadDoFn(String invalidPayloadKeyword, TupleTag<KV<String, Long>> droppedOffsetTag) {
        this.invalidPayloadKeyword = Objects.requireNonNull(invalidPayloadKeyword,
                "invalidPayloadKeyword must not be null");
        this.droppedOffsetTag = droppedOffsetTag;
    }

    @SuppressWarnings("unused") // Invoked by Beam runtime via @ProcessElement
    @ProcessElement
    public void processElement(ProcessContext context) {
        KafkaEventEnvelope envelope = Objects.requireNonNull(context.element());
        if (envelope.getPayload() != null && envelope.getPayload().contains(invalidPayloadKeyword)) {
            LOG.warn("Dropping envelope with invalid payload keyword [{}] topic={} partition={} offset={}",
                    invalidPayloadKeyword, envelope.getTopic(), envelope.getPartition(), envelope.getOffset());
            if (droppedOffsetTag != null) {
                context.output(droppedOffsetTag, KV.of(envelope.getPartitionKey(), envelope.getOffset()));
            }
            return;
        }
        context.output(envelope);
    }
}

