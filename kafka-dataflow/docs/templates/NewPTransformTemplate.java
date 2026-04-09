package org.fusadora.dataflow.ptransform;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.fusadora.dataflow.dofn.NewDoFnTemplate;

import java.util.Objects;

/**
 * Template for graph-level composition.
 *
 * Keep orchestration here (windowing, flattening, sequencing) and push single-purpose
 * record logic into DoFns.
 *
 * @param <InputT> input element type
 */
public class NewPTransformTemplate<InputT>
        extends PTransform<PCollection<InputT>, PDone> {

    private final String name;

    public NewPTransformTemplate(String name) {
        this.name = Objects.requireNonNull(name, "name must not be null");
    }

    @Override
    public PDone expand(PCollection<InputT> input) {
        input.apply("Apply DoFn [" + name + "]", ParDo.of(new NewDoFnTemplate<>(name)));
        return PDone.in(input.getPipeline());
    }
}
