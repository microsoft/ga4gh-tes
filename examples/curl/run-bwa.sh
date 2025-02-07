#!/bin/bash

# Load .env file
if [ -f .env ]; then
  export $(cat .env | xargs)
fi

# Function to submit a task
submit_task() {
  local url="$1"
  local payload="$2"

  response=$(curl -v -s -X POST "$url/v1/tasks" \
    -H "Content-Type: application/json" \
    -d "$payload" \
    --user "${TES_SERVER_USER}:${TES_SERVER_PASSWORD}" \
    --connect-timeout 5)

  echo "$response"
}

# Function to get task state
get_task_state() {
  local url="$1"
  local task_id="$2"

  curl -s "$url/v1/tasks/$task_id" \
    --user "${TES_SERVER_USER}:${TES_SERVER_PASSWORD}" \
    --connect-timeout 5 | jq -r '.state'
}

# Read the TES instance URL from .tes_instances (expecting only 1 line)
tes_instance=$(head -n 1 .tes_instances | cut -d',' -f2)
if [ -z "$tes_instance" ]; then
  echo "No TES instance defined in .tes_instances. Aborting."
  exit 1
fi

echo "Using TES instance: $tes_instance"

# Construct task payload
output_url="/${TES_OUTPUT_STORAGE_ACCT}/outputs/curl/H06HDADXX130110.1.ATCACGAT.20k.bam"

echo "$output_url"

task_payload=$(jq -n --arg output_url "$output_url" '
{
  "inputs": [
    {
      "name": "H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq",
      "path": "/data/H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq",
      "url": "https://datasettestinputs.blob.core.windows.net/dataset/seq-format-conversion/NA12878_20k/H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq"
    },
    {
      "name": "H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq",
      "path": "/data/H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq",
      "url": "https://datasettestinputs.blob.core.windows.net/dataset/seq-format-conversion/NA12878_20k/H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq"
    },
    {
      "name": "Homo_sapiens_assembly38.fasta",
      "path": "/data/Homo_sapiens_assembly38.fasta",
      "url": "https://datasettestinputs.blob.core.windows.net/dataset/references/hg38/v0/Homo_sapiens_assembly38.fasta"
    }
  ],
  "outputs": [
    {
      "name": "H06HDADXX130110.1.ATCACGAT.20k.bam",
      "path": "/data/H06HDADXX130110.1.ATCACGAT.20k.bam",
      "url": $output_url
    }
  ],
  "executors": [
    {
      "image": "quay.io/biocontainers/bwa:0.7.18--he4a0461_1",
      "command": [
        "/bin/sh", "-c",
        "bwa index /data/Homo_sapiens_assembly38.fasta && bwa mem -t 16 /data/Homo_sapiens_assembly38.fasta /data/H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq /data/H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq > /data/H06HDADXX130110.1.ATCACGAT.20k.bam"
      ],
      "workdir": "/data"
    }
  ],
  "resources": {
    "cpu_cores": 16,
    "ram_gb": 32
  }
}')


# Submit task and capture the full response
echo "Submitting task to TES instance: $tes_instance"
response=$(submit_task "$tes_instance" "$task_payload")

# Log the full response for debugging
echo "Response from TES server: $response"

# Extract task ID from the response
task_id=$(echo "$response" | jq -r '.id')

if [[ "$task_id" == null || -z "$task_id" ]]; then
  echo "Failed to submit task. Response: $response"
  exit 1
else
  echo "Task submitted successfully. Task ID: $task_id"
fi

# Monitor task state
FINAL_STATES=("COMPLETE" "EXECUTOR_ERROR" "SYSTEM_ERROR" "CANCELLED")
while true; do
  echo "Waiting for 5 seconds..."
  sleep 5

  state=$(get_task_state "$tes_instance" "$task_id")
  echo "Task ID $task_id is in state: $state"

  if [[ " ${FINAL_STATES[@]} " =~ " ${state} " ]]; then
    echo "Task has reached a final state: $state"
    break
  fi
done

echo "Done"
