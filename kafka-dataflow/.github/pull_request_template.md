## What Changed
- 

## Why
- 

## Architecture Checklist
- [ ] I followed `.github/copilot-instructions.md`.
- [ ] Package/layer boundaries are preserved.
- [ ] No `dofn -> ptransform` compile-time dependency was introduced.
- [ ] Shared logic is in `core` (or `common` for constants).
- [ ] DTOs remain pure data holders (no config loading / IO side effects).
- [ ] Any new constants are centralized under `org.fusadora.dataflow.common`.

## Checkpointing / Offset Checklist (if applicable)
- [ ] Checkpoint updates are monotonic and idempotent.
- [ ] Contiguous progression logic is preserved (or explicitly documented if changed).
- [ ] Gap handling behavior is explicit (detect/timeout/audit/continue policy).

## Tests
- [ ] Added/updated unit tests for new DoFn/PTransform behavior.
- [ ] `mvn test` passes locally.
- [ ] `LayeredArchitectureTest` passes locally.

## Operational Notes
- [ ] Any behavior change is called out in `DESIGN.md`.
- [ ] Any new config key is documented and has a sensible default/fallback.

