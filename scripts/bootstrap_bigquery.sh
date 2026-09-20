#!/usr/bin/env bash

set -euo pipefail

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID belum diset}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SQL_DIR="${PROJECT_ROOT}/sql"

LOCATION="asia-southeast2"

run_sql_file() {
    local sql_file="$1"

    if [[ ! -f "${sql_file}" ]]; then
        echo "ERROR: SQL file tidak ditemukan: ${sql_file}" >&2
        exit 1
    fi

    echo "Executing: ${sql_file}"

    bq query \
        --project_id="${GCP_PROJECT_ID}" \
        --location="${LOCATION}" \
        --use_legacy_sql=false \
        < "${sql_file}"
}

echo
echo "[1/3] Creating datasets"
run_sql_file "${SQL_DIR}/datasets.sql"

echo
echo "[2/3] Creating raw tables"

for sql_file in "${SQL_DIR}/raw/"*.sql; do
    run_sql_file "${sql_file}"
done

echo
echo "[3/3] Creating ops tables"

for sql_file in "${SQL_DIR}/ops/"*.sql; do
    run_sql_file "${sql_file}"
done

echo "Bootstrap completed successfully."