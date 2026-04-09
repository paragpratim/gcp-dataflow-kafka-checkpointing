package org.fusadora.dataflow.dofn;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * Filters out invalid extracted offset rows.
 */
@SuppressWarnings("unused") // Instantiated from pipeline transform wiring
public class DropInvalidHandledRowsFn extends DoFn<KV<String, Long>, KV<String, Long>> {

    private static final long INVALID_OFFSET = Long.MIN_VALUE;

    @SuppressWarnings("unused") // Invoked by Beam runtime via @ProcessElement
    @ProcessElement
    public void processElement(ProcessContext context) {
        KV<String, Long> kv = context.element();
        if (!kv.getKey().isBlank() && kv.getValue() != INVALID_OFFSET) {
            context.output(kv);
        }
    }
}


