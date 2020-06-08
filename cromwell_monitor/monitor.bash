#!/usr/bin/env bash

# always exit cleanly
trap "exit 0" INT TERM EXIT

while :; do

tr -d '\n' <<JSON | bq insert broad-dsde-cromwell-dev:wa_196_mvp_anyone_insert_kshakir.runtime_metrics
{
"google_project": "$(gcloud config get-value project)",
"workflow_id": "${WORKFLOW_ID:-unknown_workflow_id}",
"call_name": "${TASK_CALL_NAME:-unknown_call_name}",
"call_attempt": ${TASK_CALL_ATTEMPT:-0},
"call_index": $(echo ${TASK_CALL_INDEX:-NA} | sed "s/^NA$/null/"),
"cpu_load": $(cut -f 1 -d \  /proc/loadavg),
"cpu_count": $(grep -c processor /proc/cpuinfo),
"mem_used": $(expr $(grep '^MemTotal:' /proc/meminfo | awk -vOFMT="%.0f" "{print \$2 * 1024 }") - $(grep '^MemFree:' /proc/meminfo | awk -vOFMT="%.0f" "{print \$2 * 1024 }")),
"mem_total": $(grep '^MemTotal:' /proc/meminfo | awk -vOFMT="%.0f" "{print \$2 * 1024}"),
"disk_used": $(tail -n 1 <(echo null; df -k /cromwell_root | tail -n 1 | awk -vOFMT="%.0f" "{print \$(NF-3) * 1024}")),
"disk_total": $(tail -n 1 <(echo null; df -k /cromwell_root | tail -n 1 | awk -vOFMT="%.0f" "{print \$(NF-4) * 1024}")),
"datetime": "$(date --utc +"%Y-%m-%dT%H:%M:%SZ")"
}
JSON

sleep 60

done
