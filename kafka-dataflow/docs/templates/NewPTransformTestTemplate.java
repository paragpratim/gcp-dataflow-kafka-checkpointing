package org.fusadora.dataflow.ptransform;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/**
 * Template unit test for a PTransform.
 *
 * Replace {@code NewPTransformTemplate<String>} with the concrete PTransform under test
 * and adjust input/expected values accordingly.
 */
public class NewPTransformTestTemplate {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void verifyTransformWiring() {
        PCollection<String> input = pipeline
                .apply("CreateInput", Create.of("input"));

        input.apply("ApplyTransform", new NewPTransformTemplate<>("sample"));

        // TODO: add assertions relevant to your transform outputs.
        PAssert.that(input).containsInAnyOrder("input");

        pipeline.run().waitUntilFinish();
    }
}
