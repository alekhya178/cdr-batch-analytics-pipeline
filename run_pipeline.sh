#!/bin/bash
# Command Translation Layer for CDR Batch Analytics Pipeline

set -e

# Support arguments
QUERY_NAME=$1

if [ -z "$QUERY_NAME" ]; then
    echo "Usage: $0 <query_name>"
    echo "Supported query names: top_callers, tower_heatmap, anomalous_calls, revenue_recon"
    exit 1
fi

# Map logical queries to Airflow DAG IDs
case "$QUERY_NAME" in
    "top_callers")
        DAG_ID="top_callers_by_spend_dag"
        ;;
    "tower_heatmap")
        DAG_ID="tower_utilization_heatmap_dag"
        ;;
    "anomalous_calls")
        DAG_ID="anomalous_call_detection_dag"
        ;;
    "revenue_recon")
        DAG_ID="revenue_reconciliation_dag"
        ;;
    *)
        echo "Error: Unknown logical query '$QUERY_NAME'."
        echo "Supported query names: top_callers, tower_heatmap, anomalous_calls, revenue_recon"
        exit 1
        ;;
esac

# Generate RUN_ID in format YYYYMMDD_HHMMSS
RUN_ID=$(date -u +"%Y%m%d_%H%M%S")
echo "Generated Run ID: $RUN_ID"
echo "Mapping query '$QUERY_NAME' to DAG '$DAG_ID'..."

# Trigger the DAG via Airflow CLI
# Check if airflow command exists locally, otherwise run via docker compose
if command -v airflow >/dev/null 2>&1; then
    echo "Executing locally..."
    airflow dags trigger -r "$RUN_ID" --conf '{"run_id":"'"$RUN_ID"'"}' "$DAG_ID"
else
    echo "Executing inside docker-compose container..."
    if docker compose version >/dev/null 2>&1; then
        docker compose exec -T airflow airflow dags trigger -r "$RUN_ID" --conf '{"run_id":"'"$RUN_ID"'"}' "$DAG_ID"
    else
        docker-compose exec -T airflow airflow dags trigger -r "$RUN_ID" --conf '{"run_id":"'"$RUN_ID"'"}' "$DAG_ID"
    fi
fi

# Define state checker helper
get_dag_run_state() {
    if command -v airflow >/dev/null 2>&1; then
        python3 -c "from airflow.models import DagRun; dr = DagRun.find(dag_id='$DAG_ID', run_id='$RUN_ID'); print(dr[0].state if dr else 'NOT_FOUND')"
    else
        if docker compose version >/dev/null 2>&1; then
            docker compose exec -T airflow python3 -c "from airflow.models import DagRun; dr = DagRun.find(dag_id='$DAG_ID', run_id='$RUN_ID'); print(dr[0].state if dr else 'NOT_FOUND')" 2>/dev/null
        else
            docker-compose exec -T airflow python3 -c "from airflow.models import DagRun; dr = DagRun.find(dag_id='$DAG_ID', run_id='$RUN_ID'); print(dr[0].state if dr else 'NOT_FOUND')" 2>/dev/null
        fi
    fi
}

# Polling loop
echo "Monitoring DAG run '$RUN_ID' for completion..."
while true; do
    STATE=$(get_dag_run_state)
    STATE=$(echo "$STATE" | tr -d '\r' | xargs) # trim whitespace/newlines
    
    if [ "$STATE" = "success" ]; then
        echo "=================================================="
        echo "SUCCESS: DAG run $RUN_ID completed successfully!"
        echo "=================================================="
        exit 0
    elif [ "$STATE" = "failed" ]; then
        echo "=================================================="
        echo "FAILURE: DAG run $RUN_ID failed!"
        echo "=================================================="
        exit 1
    elif [ "$STATE" = "NOT_FOUND" ]; then
        # Give some time for the database to register the execution
        sleep 2
    else
        echo "Current DAG status: $STATE... (waiting)"
        sleep 5
    fi
done
