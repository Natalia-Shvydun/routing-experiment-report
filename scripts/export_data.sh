#!/bin/bash
#
# Runs the routing experiment notebook on Databricks, downloads the exported
# JSON, and copies it to public/data.json so the frontend uses real data.
#
# Usage:
#   ./scripts/export_data.sh
#
# Prerequisites:
#   - databricks CLI authenticated (run `databricks auth login` if token expired)
#   - DBFS or workspace access to the cluster
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

WORKSPACE_HOST="https://dbc-d10db17d-b6c4.cloud.databricks.com"
NOTEBOOK_PATH="/Users/natalia.shvydun@getyourguide.com/routing_experiment_report_data"
DBFS_OUTPUT="/tmp/report_data.json"
LOCAL_OUTPUT="$PROJECT_ROOT/public/data.json"

echo "==> Submitting notebook run to Databricks..."
RUN_ID=$(databricks jobs submit \
  --json "{
    \"run_name\": \"report-data-export\",
    \"existing_cluster_id\": \"$(databricks clusters list --output json | python3 -c "import sys,json; clusters=[c for c in json.load(sys.stdin) if c.get('state')=='RUNNING']; print(clusters[0]['cluster_id'] if clusters else '')")\",
    \"notebook_task\": {
      \"notebook_path\": \"$NOTEBOOK_PATH\"
    }
  }" --output json | python3 -c "import sys,json; print(json.load(sys.stdin)['run_id'])")

echo "    Run ID: $RUN_ID"
echo "==> Waiting for run to complete..."

while true; do
  STATE=$(databricks runs get --run-id "$RUN_ID" --output json \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['state']['life_cycle_state'])")
  
  if [ "$STATE" = "TERMINATED" ]; then
    RESULT=$(databricks runs get --run-id "$RUN_ID" --output json \
      | python3 -c "import sys,json; print(json.load(sys.stdin)['state']['result_state'])")
    if [ "$RESULT" = "SUCCESS" ]; then
      echo "    Run completed successfully."
      break
    else
      echo "    Run failed with result: $RESULT"
      databricks runs get --run-id "$RUN_ID" --output json \
        | python3 -c "import sys,json; s=json.load(sys.stdin)['state']; print(s.get('state_message',''))"
      exit 1
    fi
  elif [ "$STATE" = "INTERNAL_ERROR" ] || [ "$STATE" = "SKIPPED" ]; then
    echo "    Run ended with state: $STATE"
    exit 1
  fi
  
  echo "    State: $STATE — waiting 15s..."
  sleep 15
done

echo "==> Downloading $DBFS_OUTPUT from Databricks..."
databricks fs cp "dbfs:$DBFS_OUTPUT" "$LOCAL_OUTPUT" --overwrite

FILE_SIZE=$(wc -c < "$LOCAL_OUTPUT" | tr -d ' ')
echo "==> Done! Saved to $LOCAL_OUTPUT ($FILE_SIZE bytes)"
echo "    Start the dashboard with: npm run dev"
