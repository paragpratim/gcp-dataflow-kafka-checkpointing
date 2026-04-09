package org.fusadora.dataflow.dofn;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * org.fusadora.dataflow.dofn.DropInvalidHandledRowsFn
 * This is a Beam DoFn that filters out invalid rows from a PCollection of KV<String, Long>.
 * It checks if the key is not blank and the value is not equal to a predefined invalid offset (Long.MIN_VALUE).
 * If both conditions are met, it outputs the KV pair; otherwise, it discards it.
 *
 * @author Parag Ghosh
 * @since 10/04/2026
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


