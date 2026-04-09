# Kafka -> BigQuery Checkpointing Design (Current)

## Goal
- At-least-once processing (duplicates allowed, no silent loss).
- External checkpoint authority in Firestore (one latest checkpoint per `topic:partition`).
- Keep pipeline live under source gaps via explicit timeout policy.

## Current Flow (Implemented)

## Diagram (Excalidraw)
- File: `docs/checkpoint-processing.excalidraw`
- Quick preview: `docs/checkpoint-processing.svg`
- Focus areas:
  - handled-offset derivation from BigQuery WriteResult (success + failure)
  - merge with gap-timeout offsets before commit
  - GlobalWindows commit frontier behavior
  - Firestore checkpoint monotonic update (`nextOffsetToRead`)
  - restart bootstrap path from Firestore checkpoint to Kafka consumer-group offsets

```text
Kafka -> KafkaToMessageTransform (KafkaRecord -> KafkaEventEnvelope)
      -> SelectContiguousOffsetsWithGapEventsTransform
           - main output: contiguous envelopes
           - side output: gap-timeout offsets (missing source offsets after timeout)
      -> Window(Fixed 10s) on main output
      -> WriteRawMessageTransform (BigQueryIO)
           - success rows
           - failed rows (captured + logged)
      -> ExtractHandledWriteOffsetsFn + DropInvalidHandledRowsFn
           => handled offsets from BQ results

Merge:
  handled offsets from BQ (success + failed rows)
  + gap-timeout offsets from source stage
  -> Flatten
  -> Window.into(GlobalWindows())   # critical: single state scope for commits
  -> CommitHandledOffsetsTransform
       -> CommitContiguousHandledOffsetsDoFn
       -> CheckpointService.updateOffsetCheckpoint(...)
```

## Why GlobalWindows Before Commit
Beam state is scoped by **key + window**. If handled offsets are committed from separate windows, contiguous progression can split and stall.

Using `GlobalWindows` before `CommitHandledOffsetsTransform` ensures each `topic:partition` has one contiguous commit state timeline.

## Checkpoint Store Semantics (Firestore)
- Document id: `topic:partition`.
- Fields: `topic`, `partition`, `nextOffsetToRead`, `lastAckedOffset`, `updatedAt`, `updatedByJobId`.
- No history table by default (latest checkpoint only).
- Update is monotonic and transactional: never move checkpoint backward.

## Top-Level DoFn Ownership
- Source gap/contiguous logic: `GapAwareOffsetDoFn`
- Kafka mapping: `KafkaRecordToEnvelopeDoFn`
- BQ row mapping: `KafkaEnvelopeToTableRowDoFn`
- BQ failure extraction: `ExtractFailedRowsDoFn`
- Handled offset extraction/filter: `ExtractHandledWriteOffsetsFn`, `DropInvalidHandledRowsFn`
- Commit frontier: `CommitContiguousHandledOffsetsDoFn`

## Expected Behavior Examples

### 1) Normal success path
Input offsets for `test_df:0`: `0,1,2` (all BQ success)
- Commit frontier advances to `2`
- Firestore stores `nextOffsetToRead=3`

### 2) BQ partial failure but durable handling
Input offsets: `0,1,2`, BQ row for `1` fails
- `0,2` from success + `1` from failed-row handled stream
- Merged handled offsets become contiguous `0,1,2`
- Firestore stores `nextOffsetToRead=3`

### 3) Source gap timeout
Input observed: `0,2` (missing `1`), timeout reached
- `GapAwareOffsetDoFn` emits gap-timeout handled offset `1`
- Commit stream gets `0,1,2` after merge
- Firestore stores `nextOffsetToRead=3`

### 4) Out-of-order handled arrival
Handled stream arrives as: `3,1,2` with expected `1`
- Commit DoFn buffers until contiguous
- Commits only when frontier is contiguous
- No backward or skipping commit without explicit handling event

## Operational Notes
- If BigQuery succeeds but checkpoint update fails, replay can happen on restart (duplicates expected).
- Gap-timeout events are explicit policy decisions; they should be auditable.
- This design intentionally favors liveness with explicit, observable tradeoffs.

## Contributor Guidance
- For all new Beam/Dataflow changes, follow the architecture and coding conventions in `.github/copilot-instructions.md`.
- Keep package boundaries strict (`pipelines`, `ptransform`, `dofn`, `core`, `services`, `dto`, `common`, `di`).
- Do not introduce `dofn -> ptransform` dependencies; shared logic belongs in `core` and constants in `common`.
- Keep DTOs pure data holders; config/resource loading belongs in `utilities`.

