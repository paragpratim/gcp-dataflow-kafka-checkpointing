package org.fusadora.dataflow.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

class LayeredArchitectureTest {

    private static final JavaClasses MAIN_CLASSES = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_JARS)
            .importPackages("org.fusadora.dataflow");

    @Test
    void dofnMustNotDependOnPtransform() {
        noClasses().that().resideInAPackage("..dofn..")
                .should().dependOnClassesThat().resideInAPackage("..ptransform..")
                .because("DoFns must remain single-purpose and independent from graph composition classes")
                .check(MAIN_CLASSES);
    }

    @Test
    void dtoMustRemainPureAndNotDependOnExecutionLayers() {
        noClasses().that().resideInAPackage("..dto..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "..services..",
                        "..pipelines..",
                        "..ptransform..",
                        "..dofn..",
                        "..di..")
                .because("DTOs are pure data holders and must not depend on runtime orchestration layers")
                .check(MAIN_CLASSES);
    }

    @Test
    void ptransformMustNotDependOnPipelines() {
        noClasses().that().resideInAPackage("..ptransform..")
                .should().dependOnClassesThat().resideInAPackage("..pipelines..")
                .because("Transforms are reusable graph components and must not depend on pipeline entrypoints")
                .check(MAIN_CLASSES);
    }

    @Test
    void serviceImplementationsMustNotLeakOutsideServicesAndDi() {
        noClasses().that().resideOutsideOfPackages("..services..", "..services.impl..", "..di..")
                .should().dependOnClassesThat().resideInAPackage("..services.impl..")
                .because("Concrete service implementations should stay behind interfaces and DI wiring")
                .check(MAIN_CLASSES);
    }
}

