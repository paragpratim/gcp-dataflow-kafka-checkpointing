package org.fusadora.dataflow.dofn;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/**
 * Template unit test for a DoFn.
 *
 * Replace {@code NewDoFnTemplate<String, String>} with the concrete DoFn under test
 * and adjust input/expected values accordingly.
 */
public class NewDoFnTestTemplate {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void verifyDoFnBehavior() {
        NewDoFnTemplate<String, String> doFn = new NewDoFnTemplate<>("sample");

        PCollection<String> output = pipeline
                .apply("CreateInput", Create.of("input"))
                .apply("ApplyDoFn", ParDo.of(doFn));

        // TODO: replace expected values.
        PAssert.that(output).empty();
        pipeline.run().waitUntilFinish();
    }
}
