# BigQuery Schema Evolution & Table Extension Behavior

## Question
**If a table already exists, does my pipeline extend the table and add the new fields?**

## Quick Answer
**Your pipeline will NOT automatically extend an existing BigQuery table with new fields.** However, the `ignoreUnknownValues()` configuration allows the pipeline to continue writing without errors. New fields in your schema will simply be ignored for existing tables.

---

## Current Configuration in Your Pipeline

Your `OutputServiceImpl.writeToBqFileLoad()` method uses these key BigQuery I/O settings:

```java
BigQueryIO.writeTableRows()
    .to(bqTableName)
    .withSchema(bqTableSchema)
    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
    .ignoreUnknownValues()  // <-- This is key
    .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
    // ... other configs
```

### What Each Setting Does

| Setting | Behavior |
|---------|----------|
| `CREATE_IF_NEEDED` | Creates the table if it doesn't exist using your schema. **Does NOT update if table already exists.** |
| `WRITE_APPEND` | Appends rows to existing table. **Does NOT modify schema.** |
| `ignoreUnknownValues()` | **Critical**: Silently drops any fields in your rows that don't exist in the BigQuery table schema. |
| `STORAGE_WRITE_API` | Uses BigQuery Storage Write API instead of legacy load jobs. |

---

## What This Means for Your Nested `__metadata` Field

### Scenario 1: Table Doesn't Exist (Fresh Deploy)
- ✅ Pipeline runs with `CREATE_IF_NEEDED`
- ✅ BigQuery creates table with your full schema, including `__metadata` RECORD
- ✅ Metadata fields are written and persisted
- ✅ Checkpoint extraction reads metadata from `WriteResult.getSuccessfulStorageApiInserts()`

### Scenario 2: Table Already Exists (Old Schema)
- ✅ Pipeline runs successfully (no errors)
- ❌ **BigQuery does NOT add the `__metadata` field to the existing table**
- ⚠️ `ignoreUnknownValues()` silently drops your `__metadata` data when you try to write it
- ❌ **WriteResult rows will NOT contain `__metadata`** (it was dropped)
- ❌ `ExtractHandledWriteOffsetsFn` will fall back to flat metadata fields (or fail if those don't exist either)
- ❌ **Checkpoint progression may break or use wrong metadata**

### Example of Data Loss
```
Your code tries to write:
{
  "raw_message": "hello",
  "kafka_topic": "my-topic",
  "__metadata": {
    "_meta_kafka_topic": "my-topic",
    "_meta_kafka_partition": 0,
    "_meta_kafka_offset": 1000
  }
}

BigQuery table schema (OLD):
- raw_message: STRING
- kafka_topic: STRING
(no __metadata field)

Result:
✅ Writes to BQ with ignoreUnknownValues()
❌ __metadata silently dropped
❌ WriteResult row does NOT have __metadata
```

---

## Why This Happens

Apache Beam's BigQueryIO connector has these limitations:

1. **`CREATE_IF_NEEDED` only creates**: It doesn't update existing schemas
2. **BigQuery doesn't support automatic schema evolution** in write operations for appending
3. **`ignoreUnknownValues()` is a safety mechanism**: To prevent writes from failing when data has extra fields

---

## How to Migrate Existing Tables

### Option 1: Manual Schema Update (Recommended for Production)
Use the BigQuery CLI or Console to manually update the table schema:

```bash
# Update schema via bq command-line tool
bq update \
  --schema=./raw_message_schema.txt \
  dataset_name.KAFKA_RAW_MESSAGE

# Or use gcloud API
gcloud bigquery tables update dataset_name.KAFKA_RAW_MESSAGE \
  --schema-from-file=raw_message_schema.txt
```

### Option 2: Drop and Recreate (For Development)
**Only if the table contains non-critical data:**

```bash
# Delete old table
bq rm -t dataset_name.KAFKA_RAW_MESSAGE

# Pipeline will recreate it with new schema on next run
# (CREATE_IF_NEEDED will trigger)
```

### Option 3: Use a New Table Name
Deploy to a new table instead of modifying existing one:

1. Update `tableName` in `src/main/resources/topic-config.json` for the topic to a new value (for example `kafka_raw_message_test_df_v2`)
2. Deploy pipeline (creates fresh table with new schema)
3. Migrate data if needed using BigQuery INSERT SELECT queries

---

## Verification Steps

After updating the schema, verify the `__metadata` field is being written:

### 1. Check Table Schema
```bash
bq show --schema dataset_name.KAFKA_RAW_MESSAGE
```

Expected output includes:
```json
{
  "mode": "NULLABLE",
  "name": "__metadata",
  "type": "RECORD",
  "fields": [
    {"mode": "NULLABLE", "name": "_meta_kafka_topic", "type": "STRING"},
    {"mode": "NULLABLE", "name": "_meta_kafka_partition", "type": "INTEGER"},
    {"mode": "NULLABLE", "name": "_meta_kafka_offset", "type": "INTEGER"}
  ]
}
```

### 2. Query Sample Row
```sql
SELECT raw_message, __metadata FROM dataset_name.KAFKA_RAW_MESSAGE 
LIMIT 1;
```

Expected output:
```
raw_message: "hello"
__metadata:  {_meta_kafka_topic: "my-topic", _meta_kafka_partition: 0, _meta_kafka_offset: 1000}
```

### 3. Verify Checkpoint Progression
Check Firestore that offsets are being written:
```bash
# Query your firestore collection for checkpoints
gcloud firestore documents list checkpoint-collection --database=default
```

Should show recent timestamps and offset values increasing.

---

## Best Practices for Production Rollout

### 1. Plan Ahead
- Schema changes should be coordinated with deployment
- Decide on table migration strategy before deploying new code

### 2. Non-Breaking vs Breaking Changes
- **Non-breaking**: Adding NULLABLE fields (existing tables ignore them)
  - Add to schema in code, manually update existing tables later
  - Safe to deploy early; old tables will catch up when manually migrated

- **Breaking**: Modifying/removing fields
  - Requires coordinated deployment
  - Update all tables before deploying new code

### 3. Backward Compatibility (Already Implemented!)
Your `ExtractHandledWriteOffsetsFn` already handles both old and new schemas:

```java
// Tries new nested format first
if (metadataRecord instanceof TableRow metadataTableRow) {
    // Read from __metadata RECORD
}
// Falls back to old flat format
else {
    topic = row.get(KafkaMetadataConstants.META_KAFKA_TOPIC);
    partition = row.get(KafkaMetadataConstants.META_KAFKA_PARTITION);
    offset = row.get(KafkaMetadataConstants.META_KAFKA_OFFSET);
}
```

This means:
- ✅ You can deploy new code before updating schemas
- ✅ It will read from old flat fields until tables are migrated
- ✅ Once tables are updated, it automatically reads from nested `__metadata`

---

## Summary Table

| Action | `CREATE_IF_NEEDED` | `WRITE_APPEND` | Result |
|--------|-------------------|-----------------|--------|
| First run (no table) | ✅ Creates | - | Table created with full schema |
| Subsequent runs (table exists) | ⏭️ Skips | ✅ Appends | Data written, new fields dropped via `ignoreUnknownValues()` |
| Manual schema update then append | - | ✅ Appends | New fields now persisted in rows |

---

## Related Documentation
- [Beam BigQueryIO Schema Handling](https://beam.apache.org/documentation/io/built-in-transforms/google-cloud-platform/bigquery/)
- [BigQuery Schema Evolution](https://cloud.google.com/bigquery/docs/schema-update-options)
- [Your `ExtractHandledWriteOffsetsFn` Implementation](../src/main/java/org/fusadora/dataflow/dofn/ExtractHandledWriteOffsetsFn.java)

