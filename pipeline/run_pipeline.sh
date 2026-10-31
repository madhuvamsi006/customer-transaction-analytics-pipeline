#!/usr/bin/env bash
# =============================================================================
# run_pipeline.sh — Submit pipeline for any day using project_config.yaml
#
# Usage:
#   ./pipeline/run_pipeline.sh --day 0              # Day 0 baseline load
#   ./pipeline/run_pipeline.sh --day 1              # Day 1 incremental
#   ./pipeline/run_pipeline.sh --day 2              # Day 2 incremental
#   ./pipeline/run_pipeline.sh --day 0 --config /path/to/project_config.yaml
#   ./pipeline/run_pipeline.sh --day 0 --engine dataflow
#
# The script reads ALL project settings from project_config.yaml.
# No hardcoded values — just point to your config file.
# =============================================================================

set -euo pipefail

# ---- Defaults ---------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
CONFIG_FILE="${REPO_ROOT}/project_config.yaml"
DAY=""
ENGINE_OVERRIDE=""   # leave blank to use value from config

# ---- Parse args -------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --day)       DAY="$2";            shift 2 ;;
    --config)    CONFIG_FILE="$2";    shift 2 ;;
    --engine)    ENGINE_OVERRIDE="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

if [[ -z "$DAY" ]]; then
  echo "ERROR: --day is required (0, 1, or 2)"
  exit 1
fi

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "ERROR: Config file not found: $CONFIG_FILE"
  exit 1
fi

# ---- Read values from config using Python -----------------------------------
read_cfg() {
  python3 -c "
import yaml, sys
with open('${CONFIG_FILE}') as f:
    c = yaml.safe_load(f)
keys = '${1}'.split('.')
v = c
for k in keys:
    v = v[k]
print(v)
"
}

PROJECT_ID=$(read_cfg "gcp.project_id")
REGION=$(read_cfg "gcp.region")
BUCKET=$(read_cfg "gcs.bucket")
DATASET=$(read_cfg "bigquery.dataset")
BATCH_PREFIX=$(read_cfg "pipeline.batch_id_prefix")
ENGINE=$(read_cfg "engine.type")
CLUSTER=$(read_cfg "engine.dataproc.cluster_name")
IMAGE_VERSION=$(read_cfg "engine.dataproc.image_version")
MASTER_TYPE=$(read_cfg "engine.dataproc.master_machine_type")
WORKER_TYPE=$(read_cfg "engine.dataproc.worker_machine_type")
NUM_WORKERS=$(read_cfg "engine.dataproc.num_workers")
AUTO_DELETE=$(read_cfg "engine.dataproc.auto_delete_cluster")
ENV=$(read_cfg "environment")

# Override engine if passed via CLI
if [[ -n "$ENGINE_OVERRIDE" ]]; then
  ENGINE="$ENGINE_OVERRIDE"
fi

BATCH_ID="${BATCH_PREFIX}_DAY${DAY}"
GCS_CODE="gs://${BUCKET}/code"

echo "========================================================"
echo " Customer Transaction Analytics Pipeline"
echo "========================================================"
echo " Config      : $CONFIG_FILE"
echo " Environment : $ENV"
echo " Project     : $PROJECT_ID"
echo " Region      : $REGION"
echo " Bucket      : gs://${BUCKET}"
echo " BQ Dataset  : ${DATASET}"
echo " Engine      : ${ENGINE}"
echo " Load Day    : Day ${DAY}  (Batch: ${BATCH_ID})"
echo "========================================================"

# ---- Upload code to GCS -----------------------------------------------------
echo "[1/4] Syncing code to GCS..."
gsutil -m rsync -r "${REPO_ROOT}/src"    "${GCS_CODE}/src"
gsutil cp "${REPO_ROOT}/pipeline/dataproc_pipeline.py" "${GCS_CODE}/dataproc_pipeline.py"
gsutil cp "${CONFIG_FILE}"               "${GCS_CODE}/project_config.yaml"

# ---- Upload source data (only on Day 0) ------------------------------------
if [[ "$DAY" == "0" ]]; then
  echo "[2/4] Uploading source data files to GCS..."
  INPUT_PREFIX=$(read_cfg "gcs.input_prefix")
  for d in 0 1 2; do
    LOCAL_FILE="${REPO_ROOT}/data/day${d}.txt"
    if [[ -f "$LOCAL_FILE" ]]; then
      gsutil cp "$LOCAL_FILE" "gs://${BUCKET}/${INPUT_PREFIX}/day${d}/day${d}.txt"
      echo "  Uploaded day${d}.txt"
    fi
  done
else
  echo "[2/4] Skipping data upload (already uploaded on Day 0)"
fi

# ---- Create Dataproc cluster if needed (Dataproc engine) --------------------
if [[ "$ENGINE" == "dataproc" ]]; then
  CLUSTER_EXISTS=$(gcloud dataproc clusters list \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --filter="clusterName=${CLUSTER} AND status.state=RUNNING" \
    --format="value(clusterName)" 2>/dev/null || echo "")

  if [[ -z "$CLUSTER_EXISTS" ]]; then
    echo "[3/4] Creating Dataproc cluster: ${CLUSTER}..."
    gcloud dataproc clusters create "$CLUSTER" \
      --project="$PROJECT_ID" \
      --region="$REGION" \
      --master-machine-type="$MASTER_TYPE" \
      --worker-machine-type="$WORKER_TYPE" \
      --num-workers="$NUM_WORKERS" \
      --image-version="$IMAGE_VERSION" \
      --optional-components=JUPYTER \
      --enable-component-gateway \
      --labels="environment=${ENV},pipeline=txn-analytics"
  else
    echo "[3/4] Dataproc cluster '${CLUSTER}' already running — reusing."
  fi

  # ---- Submit PySpark job --------------------------------------------------
  echo "[4/4] Submitting PySpark job for Day ${DAY}..."
  gcloud dataproc jobs submit pyspark "${GCS_CODE}/dataproc_pipeline.py" \
    --cluster="$CLUSTER" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --labels="environment=${ENV},load_day=day${DAY}" \
    -- \
    --config="${GCS_CODE}/project_config.yaml" \
    --load_day="$DAY"

  # ---- Auto-delete cluster if configured ----------------------------------
  if [[ "$AUTO_DELETE" == "True" || "$AUTO_DELETE" == "true" ]]; then
    echo "Auto-delete enabled — deleting cluster after job..."
    gcloud dataproc clusters delete "$CLUSTER" \
      --region="$REGION" \
      --project="$PROJECT_ID" \
      --quiet
  fi

elif [[ "$ENGINE" == "dataflow" ]]; then
  echo "[3/4] Preparing Dataflow submission..."
  SA=$(read_cfg "gcp.service_account" 2>/dev/null || echo "")
  MAX_WORKERS=$(read_cfg "engine.dataflow.max_workers")
  MACHINE_TYPE=$(read_cfg "engine.dataflow.machine_type")

  SA_FLAG=""
  if [[ -n "$SA" ]]; then
    SA_FLAG="--service_account_email=${SA}"
  fi

  echo "[4/4] Launching Dataflow job for Day ${DAY}..."
  python3 "${REPO_ROOT}/pipeline/dataflow_pipeline.py" \
    --runner=DataflowRunner \
    --project="$PROJECT_ID" \
    --region="$REGION" \
    --temp_location="gs://${BUCKET}/temp" \
    --staging_location="gs://${BUCKET}/staging" \
    --machine_type="$MACHINE_TYPE" \
    --max_num_workers="$MAX_WORKERS" \
    --config="${CONFIG_FILE}" \
    --load_day="$DAY" \
    $SA_FLAG

else
  echo "ERROR: Unknown engine '${ENGINE}'. Use 'dataproc' or 'dataflow' in project_config.yaml"
  exit 1
fi

echo ""
echo "Pipeline complete for Day ${DAY}. Verify in BigQuery:"
echo "  bq query --use_legacy_sql=false 'SELECT * FROM \`${PROJECT_ID}.${DATASET}.kpi_top_5_customers_by_volume\` LIMIT 10'"
