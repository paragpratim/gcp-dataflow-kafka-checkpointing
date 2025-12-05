#!/usr/bin/env bash
set -euo pipefail

print_usage() {
  cat <<EOF
Usage: $0 --project PROJECT --region REGION --stagingLocation GS://BUCKET/path --tempLocation GS://BUCKET/path --pipelineName NAME [--profile PROFILE] [--runner RunnerClass] [--skip-build] [--help] -- [extra beam args]

Description:
  Builds (unless --skip-build) the `kafka-dataflow` shaded runner JAR and
  runs the pipeline main class (`org.fusadora.dataflow.pipelines.PipelineRunner`).

Required:
  --project           GCP project id
  --region            GCP region (e.g. us-central1)
  --stagingLocation   GCS staging path (gs://.../staging)
  --tempLocation      GCS temp path (gs://.../temp)
  --pipelineName      Pipeline name (required by the pipeline options)

Optional:
  --profile           Maven profile to build with (default: local)
  --runner            Beam runner class (default: org.apache.beam.runners.dataflow.DataflowRunner)
  --skip-build        Skip building the jar (use if already built)
  --subnetwork        (optional) VPC subnetwork for Dataflow workers (e.g. regions/REGION/subnetworks/SUBNET)
  --serviceAccount    (optional) Service account email for Dataflow workers
  --streaming         (optional) Run the job as a streaming job (for unbounded sources)
  --enable-streaming-engine
                      (optional) Enable Dataflow Streaming Engine (adds --experiments=enable_streaming_engine)
  --use-streaming-engine
                      (optional) Convenience flag: implies --streaming and --enable-streaming-engine
  --maxNumWorkers N    (optional) Maximum number of workers (Dataflow option, default: 2)
  --noPublicIps        (optional) Disable public IPs for Dataflow workers (for orgs that block external IPs)
  --help, -h          Show this help

Examples:
  # Build and run on Dataflow
  ./scripts/run_dataflow_job.sh --project my-project --region us-central1 \
    --stagingLocation gs://my-project-dataflow/staging --tempLocation gs://my-project-dataflow/temp \
    --pipelineName my-pipeline

  # Skip build and pass extra Beam args
  ./scripts/run_dataflow_job.sh --project my-project --region us-central1 \
    --stagingLocation gs://... --tempLocation gs://... --pipelineName my-pipeline --skip-build -- --jobName=my-job-123
EOF
}

# Defaults
PROFILE="local"
RUNNER="org.apache.beam.runners.dataflow.DataflowRunner"
SKIP_BUILD=false
SUBNETWORK=""
SERVICE_ACCOUNT=""
STREAMING=false
ENABLE_STREAMING_ENGINE=false
USE_STREAMING_ENGINE=false
MAX_NUM_WORKERS="2"
NO_PUBLIC_IPS=false
EXTRA_ARGS=()

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --project)
      PROJECT="$2"; shift 2;;
    --region)
      REGION="$2"; shift 2;;
    --stagingLocation)
      STAGING_LOCATION="$2"; shift 2;;
    --tempLocation)
      TEMP_LOCATION="$2"; shift 2;;
    --pipelineName)
      PIPELINE_NAME="$2"; shift 2;;
    --profile)
      PROFILE="$2"; shift 2;;
    --subnetwork)
      SUBNETWORK="$2"; shift 2;;
    --serviceAccount)
      SERVICE_ACCOUNT="$2"; shift 2;;
    --streaming)
      STREAMING=true; shift;;
    --enable-streaming-engine)
      ENABLE_STREAMING_ENGINE=true; shift;;
    --use-streaming-engine)
      USE_STREAMING_ENGINE=true; shift;;
    --maxNumWorkers)
      MAX_NUM_WORKERS="$2"; shift 2;;
    --noPublicIps)
      NO_PUBLIC_IPS=true; shift;;
    --runner)
      RUNNER="$2"; shift 2;;
    --skip-build)
      SKIP_BUILD=true; shift;;
    --help|-h)
      print_usage; exit 0;;
    --)
      shift
      EXTRA_ARGS=("$@")
      break
      ;;
    *)
      echo "Unknown argument: $1" >&2
      print_usage
      exit 1
      ;;
  esac
done

# Validate required
if [[ -z "${PROJECT:-}" || -z "${REGION:-}" || -z "${STAGING_LOCATION:-}" || -z "${TEMP_LOCATION:-}" || -z "${PIPELINE_NAME:-}" ]]; then
  echo "Error: missing required parameter" >&2
  print_usage
  exit 2
fi

# Build
if [ "$SKIP_BUILD" = false ]; then
  echo "Building kafka-dataflow jar (profile: $PROFILE) ..."
  mvn -f kafka-dataflow/pom.xml clean package -DskipTests -P"$PROFILE"

  # The pom is expected to run the shade plugin during the package phase and
  # attach a shaded artifact named *-runner.jar. Don't invoke the shade goal
  # directly (it's unsupported when the main artifact isn't present). Instead
  # check for the shaded jar and retry package once if missing.
  SHADJAR=$(ls kafka-dataflow/target/*-runner.jar 2>/dev/null | head -n1 || true)
  if [[ -z "$SHADJAR" ]]; then
    echo "Shaded runner jar not found after package; retrying package once..."
    mvn -f kafka-dataflow/pom.xml clean package -DskipTests -P"$PROFILE"
    SHADJAR=$(ls kafka-dataflow/target/*-runner.jar 2>/dev/null | head -n1 || true)
    if [[ -z "$SHADJAR" ]]; then
      echo "Error: shaded runner jar not found. The pom should attach the shaded artifact during 'package'." >&2
      echo "Avoid invoking the shade goal directly; inspect the pom or build logs for failures." >&2
      exit 4
    fi
  fi
fi

# Find shaded runner jar
JAR=$(ls kafka-dataflow/target/*-runner.jar 2>/dev/null | head -n1 || true)
if [[ -z "$JAR" || ! -f "$JAR" ]]; then
  echo "Error: shaded runner jar not found in kafka-dataflow/target. Build may have failed." >&2
  exit 3
fi

echo "Using jar: $JAR"

echo "Starting Dataflow job (pipelineName=$PIPELINE_NAME) ..."

# Build invocation args and include optional flags only when provided
INVOKE_ARGS=(
  --project="$PROJECT"
  --region="$REGION"
  --stagingLocation="$STAGING_LOCATION"
  --tempLocation="$TEMP_LOCATION"
  --runner="$RUNNER"
  --pipelineName="$PIPELINE_NAME"
)

if [[ -n "${SUBNETWORK}" ]]; then
  # Normalize common full resource forms (e.g. "projects/PROJECT/regions/REGION/subnetworks/SUBNET"
  # or full compute API URLs) down to the form Dataflow expects: "regions/REGION/subnetworks/SUBNET".
  if [[ "$SUBNETWORK" == *"regions/"* ]]; then
    # Extract from the first occurrence of "regions/..." to the end
    SUBNETWORK="regions/${SUBNETWORK#*regions/}"
    echo "Normalized subnetwork to: $SUBNETWORK"
  fi
  INVOKE_ARGS+=(--subnetwork="$SUBNETWORK")
fi

if [[ -n "${SERVICE_ACCOUNT}" ]]; then
  INVOKE_ARGS+=(--serviceAccount="$SERVICE_ACCOUNT")
fi

# Forward streaming flags if requested
if [[ "$USE_STREAMING_ENGINE" == "true" ]]; then
  STREAMING=true
  ENABLE_STREAMING_ENGINE=true
fi

if [[ "$STREAMING" == "true" ]]; then
  INVOKE_ARGS+=(--streaming)
fi

if [[ "$ENABLE_STREAMING_ENGINE" == "true" ]]; then
  INVOKE_ARGS+=(--experiments=enable_streaming_engine)
fi

# Show the final java invocation (for debugging/verification)
echo "Invoking: java -jar $JAR ${INVOKE_ARGS[*]} ${EXTRA_ARGS[*]}"

# If organization policy blocks external IPs, forward the Dataflow option to disable public IPs on workers
if [[ "$NO_PUBLIC_IPS" == "true" ]]; then
  INVOKE_ARGS+=(--usePublicIps=false)
fi

# Always include maxNumWorkers (Dataflow option). This defaults to 2 but can be overridden with --maxNumWorkers.
if [[ -n "${MAX_NUM_WORKERS:-}" ]]; then
  INVOKE_ARGS+=(--maxNumWorkers="$MAX_NUM_WORKERS")
fi

java -jar "$JAR" "${INVOKE_ARGS[@]}" "${EXTRA_ARGS[@]:-}"
