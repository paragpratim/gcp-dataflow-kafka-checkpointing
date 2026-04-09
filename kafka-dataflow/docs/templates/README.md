# Beam Scaffolding Templates

Use these templates as a quick starting point for new Beam components while preserving this repository's architecture conventions.

## Available Templates

- `NewDoFnTemplate.java`
- `NewPTransformTemplate.java`
- `NewDoFnTestTemplate.java`
- `NewPTransformTestTemplate.java`

## Usage

1. Copy the template file.
2. Rename class/package/symbols.
3. Keep package boundaries:
   - DoFn classes under `org.fusadora.dataflow.dofn`
   - PTransforms under `org.fusadora.dataflow.ptransform`
   - Shared algorithms under `org.fusadora.dataflow.core`
4. Add/adjust tests in `src/test/java` before finalizing.

For project-specific conventions, always follow `.github/copilot-instructions.md`.

