# gcp-dataflow-kafka-checkpointing

This repository contains a Dataflow pipeline that reads from Kafka and writes to BigQuery, plus a helper script to build and submit the job.

**Run script**

- Path: `scripts/run_dataflow_job.sh`
- Purpose: Builds the `kafka-dataflow` shaded runner JAR and runs the `org.fusadora.dataflow.pipelines.PipelineRunner` Java main class with required Dataflow/Beam options.

**Requirements**

- Java 17 and Maven installed on your machine.
- GCP service account credentials available and pointed to by `GOOGLE_APPLICATION_CREDENTIALS` environment variable.
- GCS buckets created for `stagingLocation` and `tempLocation`.
- The service account must have permissions to run Dataflow and access the GCS buckets.

**Usage**

Required flags:
- `--project`           GCP project id
- `--region`            GCP region (e.g. `us-central1`)
- `--stagingLocation`   GCS staging path (e.g. `gs://my-bucket/staging`)
- `--tempLocation`      GCS temp path (e.g. `gs://my-bucket/temp`)
- `--pipelineName`      Pipeline name (required by the pipeline options)

Optional flags:
- `--profile`           Maven profile to build with (default: `local`)
- `--runner`            Beam runner class (default: `org.apache.beam.runners.dataflow.DataflowRunner`)
- `--skip-build`        Skip building the jar (use if already built)
 - `--subnetwork`        (optional) VPC subnetwork for Dataflow workers (e.g. `regions/REGION/subnetworks/SUBNET`)
  Accepted formats:
  - `regions/REGION/subnetworks/SUBNET` (recommended)
  - full resource form `projects/PROJECT/regions/REGION/subnetworks/SUBNET` or the full compute URL; the script will normalize these to the `regions/...` form.
 - `--serviceAccount`    (optional) Service account email for Dataflow workers
 - `--streaming`         (optional) Run the job as a streaming job (for unbounded sources)
 - `--enable-streaming-engine` (optional) Enable Dataflow Streaming Engine (adds `--experiments=enable_streaming_engine`)
 - `--use-streaming-engine` (optional) Convenience: implies `--streaming` and `--enable-streaming-engine`
 - `--maxNumWorkers` N  (optional) Maximum number of Dataflow workers (default: `2`)
 - `--noPublicIps`      (optional) Disable public IPs for Dataflow workers. Use this if your org policy blocks external IPs on VM instances.

Any additional Beam/Dataflow options can be passed after `--` and are forwarded to the Java pipeline.

**Examples**

1) Build + submit (recommended first run):

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"

./scripts/run_dataflow_job.sh \
  --project my-gcp-project \
  --region us-central1 \
  --stagingLocation gs://my-gcp-project-dataflow/staging \
  --tempLocation gs://my-gcp-project-dataflow/temp \
  --pipelineName kafka-to-bq-pipeline \
  --profile prd \
  --subnetwork regions/us-central1/subnetworks/my-subnet \
  --serviceAccount my-dataflow-sa@my-gcp-project.iam.gserviceaccount.com -- \
  --streaming --enable-streaming-engine --jobName=kafka-to-bq-$(date -u +%Y%m%d-%H%M%S)
```

2) Skip build (use when you already have the shaded jar in `kafka-dataflow/target`):

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"

./scripts/run_dataflow_job.sh \
  --project my-gcp-project \
  --region us-central1 \
  --stagingLocation gs://my-gcp-project-dataflow/staging \
  --tempLocation gs://my-gcp-project-dataflow/temp \
  --pipelineName kafka-to-bq-pipeline \
  --skip-build \
  --subnetwork regions/us-central1/subnetworks/my-subnet \
  --serviceAccount my-dataflow-sa@my-gcp-project.iam.gserviceaccount.com -- \
  --streaming --enable-streaming-engine --jobName=kafka-to-bq-$(date -u +%Y%m%d-%H%M%S)
```

**Notes**

- Replace `/path/to/service-account-key.json`, `my-gcp-project`, and GCS paths with your values.
- Ensure the service account has IAM roles required to run Dataflow and access GCS (e.g. Dataflow Worker, Storage Admin, or least privileged equivalents).

If you'd like, I can also add a Makefile target or a short `scripts/README.md` next to the script. Let me know which you prefer.
