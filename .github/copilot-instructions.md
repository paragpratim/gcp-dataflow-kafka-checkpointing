# Copilot Skills: Apache Beam Dataflow Architecture for This Repository

Use these project-specific skills whenever generating or modifying code in the kafka-dataflow java project in this repository.

## 1) Layered package structure (keep boundaries strict)

- `org.fusadora.dataflow.pipelines`: pipeline orchestration only.
- `org.fusadora.dataflow.ptransform`: composable Beam transforms (graph-level composition).
- `org.fusadora.dataflow.dofn`: single-purpose DoFns only.
- `org.fusadora.dataflow.services`: external IO contracts (Kafka, BigQuery, checkpoint store).
- `org.fusadora.dataflow.services.impl`: concrete implementations.
- `org.fusadora.dataflow.core`: shared algorithms used by DoFns/transforms (for example contiguous offset logic).
- `org.fusadora.dataflow.common`: constants and shared field names.
- `org.fusadora.dataflow.dto`: DTOs only (no config loading, no IO side effects).
- `org.fusadora.dataflow.utilities`: helper utilities (config/resource/schema helpers).
- `org.fusadora.dataflow.di`: Guice wiring.

### Boundary rules

- Never create `dofn -> ptransform` compile-time dependencies.
- Shared logic used by multiple layers must go to `core` (or `common` for constants).
- DTOs must stay pure data holders.

## 2) Beam coding patterns

- Keep DoFns top-level classes under `dofn` for direct unit testing.
- Keep each DoFn single responsibility:
  - filtering DoFn only filters,
  - mapping DoFn only maps,
  - commit DoFn only commits.
- Put graph composition (windowing, flatten, side outputs, sequencing) in PTransforms/pipeline classes.
- Prefer immutable constructor-injected dependencies for DoFns and PTransforms.
- For stateful logic, use explicit state IDs/timers and keep algorithmic pieces in `core` helpers.

## 3) Checkpoint and offset handling conventions

- At-least-once is the baseline.
- Use contiguous offset progression before updating checkpoints.
- Keep checkpoint writes monotonic (`max(current, candidate)`) and idempotent.
- Keep Kafka metadata fields in handled rows using constants from `KafkaMetadataConstants`:
  - `_meta_kafka_topic`
  - `_meta_kafka_partition`
  - `_meta_kafka_offset`
- Gap handling must be explicit (detect, timeout, audit, continue policy).

## 4) BigQuery and payload handling conventions

- Keep payload filtering separate from row mapping:
  - `FilterValidPayloadDoFn` handles filtering,
  - `KafkaEnvelopeToTableRowDoFn` handles mapping.
- Keep BigQuery write transforms generic and reusable.
- Use write results to derive handled offsets for checkpoint progression.

## 5) Config and constants conventions

- Use `PropertyUtils.getProperty(...)` for runtime config lookups.
- Do not reintroduce mixed static + injected singleton patterns for config.
- Keep domain constants in `common`.
- Do not hardcode field names in multiple places when a constant already exists.

## 6) DI and test architecture conventions

- Keep production bindings in `DataflowBusinessLogicModule`.
- Keep test overrides in `TestDataflowBusinessLogicModule`.
- Prefer stub services in `src/test/.../testing/stubs`.
- Use shared test fixtures in `KafkaTestData` and `BigQueryTestUtils`.
- Add unit tests for each new DoFn/PTransform behavior.

## 7) Change quality checklist (apply before finalizing)

- Package/layer boundaries are preserved.
- No new `dofn -> ptransform` dependencies.
- New behavior is covered by tests.
- Existing tests pass.
- Naming remains consistent with current repository style.
- No dead code or duplicate constants introduced.

## 8) Preferred implementation style

- Keep changes small, focused, and reversible.
- Preserve existing behavior unless a behavior change is explicitly requested.
- Add concise comments only where logic is not obvious.
- Keep public API surface minimal.

