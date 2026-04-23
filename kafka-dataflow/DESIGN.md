# Kafka -> BigQuery Checkpointing Design (Current)

## Goals

- At-least-once processing (duplicates are acceptable; silent loss is not).
- External checkpoint authority per `topic:partition` in Firestore.
- Liveness under source gaps and invalid payload drops.
- Predictable drain behavior with unbounded Kafka input.

## Diagram

- Current flow diagram (Mermaid): `docs/checkpoint-processing.md`
- Legacy editable diagram: `docs/checkpoint-processing.excalidraw`
- Legacy preview: `docs/checkpoint-processing.svg`

## Current Flow (Implemented)

```text
Kafka -> KafkaToMessageTransform (KafkaRecord -> KafkaEventEnvelope)
      -> SelectContiguousOffsetsWithGapEventsTransform
           - main output: contiguous envelopes
           - side output: gap-timeout handled offsets
      -> Window(Fixed 10s) on contiguous envelopes
      -> FilterValidPayloadDoFn
           - main output: valid envelopes for BigQuery
           - side output: dropped-invalid handled offsets
      -> WriteRawMessageTransform (pure map+write)
           - success rows
           - failed rows (captured + logged)
      -> ExtractHandledWriteOffsetsFn + DropInvalidHandledRowsFn
           => handled offsets from BQ results

Merge handled-offset streams:
  handled offsets from BQ (success + failed rows)
  + gap-timeout handled offsets
  + dropped-invalid handled offsets
  -> Flatten
  -> GlobalWindows + repeating processing-time trigger
  -> CommitHandledOffsetsTransform
       -> CommitContiguousHandledOffsetsDoFn
       -> CheckpointService.updateOffsetCheckpoint(...)
```

## Why GlobalWindows Before Commit

Beam state is scoped by **key + window**. Commit state must remain single-scope per key; otherwise contiguous frontier can split and stall across windows.

The commit stage uses:

- `GlobalWindows`
- repeating processing-time trigger
- `discardingFiredPanes()`

This keeps state unified and enables clean drain on unbounded sources.

## Checkpoint Commit Behavior

### Commit frontier and batching

- `CommitContiguousHandledOffsetsDoFn` tracks contiguous expected offset in keyed state.
- Out-of-order handled offsets are buffered until the frontier can advance.
- Firestore writes are timer-batched using `checkpointCommitIntervalSeconds`.

### Firestore update semantics

- Firestore document id: `topic:partition`
- Fields: `topic`, `partition`, `nextOffsetToRead`, `lastAckedOffset`, `updatedAt`, `updatedByJobId`
- Writes are async (`set(..., merge)`) to reduce worker blocking.
- Forward-only progression is enforced by Beam keyed state (monotonic frontier per key).

## Topic-Level Configuration

Each topic config supports:

- `topicName`
- `datasetName`
- `tableName` (optional; fallback: `KAFKA_RAW_MESSAGE`)
- `checkpointCommitIntervalSeconds` (optional override)

This allows per-topic BQ destination and per-topic checkpoint cadence.

## Startup Offset Loading

At pipeline setup, checkpoint offsets are loaded once per topic via `getTopicPartitionOffsets(topic)` and passed into gap/commit transforms as initial partition offsets. Missing partitions still fallback to on-demand lookup.

## Observability (Commit Stage)

Key metrics for `Commit Offsets From Handled Stream [topic]`:

- Throughput/progress: `handled_offsets_seen`, `offsets_contiguously_acked`, `contiguous_advance_size`
- Stall indicators: `no_progress_buffered`, `buffered_state_size`
- Timer behavior: `commit_timer_armed`, `commit_timer_fired`, `commit_timer_fired_without_pending_ack`
- Commit behavior: `checkpoint_commit_attempted`, `checkpoint_service_call_latency_ms`
- Firestore service metrics: `firestore_checkpoint_update_requested`, `firestore_checkpoint_update_dispatch_latency_ms`, `firestore_checkpoint_update_success`, `firestore_checkpoint_update_failure`

## Expected Behavior Examples

### 1) Normal success path

Offsets for `test_df:0`: `0,1,2` (BQ success)

- Frontier advances to `2`
- Firestore stores `nextOffsetToRead=3`

### 2) BQ failure path remains handled

Offsets: `0,1,2`, BQ write for `1` fails

- Success path yields `0,2`, failed-row path yields `1`
- Merged handled stream becomes contiguous `0,1,2`
- Firestore stores `nextOffsetToRead=3`

### 3) Source gap timeout path remains handled

Observed source offsets: `0,2` (missing `1`)

- Gap timeout emits handled offset `1`
- Commit stream gets `0,1,2`
- Firestore stores `nextOffsetToRead=3`

### 4) Invalid payload drop path remains handled

Observed contiguous offsets: `0,1,2`, where `1` contains invalid payload keyword

- Filter drops payload `1` before BQ write
- Side output emits handled offset `1`
- Commit stream still receives `0,1,2`
- Firestore stores `nextOffsetToRead=3`

## Operational Notes

- If BigQuery succeeds but checkpoint write fails, restart replay can happen (duplicates expected).
- Gap timeout and dropped-invalid offsets are explicit policy decisions and should be monitored.
- Key parallelism in commit stage is bounded by active `topic:partition` keys; adding Kafka partitions increases available parallel lanes.

## Contributor Guidance

- Follow `.github/copilot-instructions.md` for coding and architecture conventions.
- Keep package boundaries strict (`pipelines`, `ptransform`, `dofn`, `core`, `services`, `dto`, `common`, `di`).
- Keep shared state logic in `core`; keep DTOs as pure data holders.

