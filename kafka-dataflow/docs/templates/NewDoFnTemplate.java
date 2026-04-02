package org.fusadora.dataflow.dofn;

import org.apache.beam.sdk.transforms.DoFn;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Template for a single-responsibility DoFn.
 *
 * Replace InputT/OutputT with concrete types and implement only one concern
 * (filter / map / commit).
 *
 * @param <InputT>  input element type
 * @param <OutputT> output element type
 */
public class NewDoFnTemplate<InputT, OutputT> extends DoFn<InputT, OutputT> implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String name;

    public NewDoFnTemplate(String name) {
        this.name = Objects.requireNonNull(name, "name must not be null");
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        InputT element = context.element();
        // TODO: implement one responsibility only.
        // Output example:
        // OutputT mapped = transform(element);
        // context.output(mapped);
    }

    protected String getName() {
        return name;
    }
}
