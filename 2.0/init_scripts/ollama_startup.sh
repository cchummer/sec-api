#!/bin/bash

set -eo pipefail

IFS=',' read -ra MODELS <<< "${INITIAL_MODELS:-mistral:7b-instruct-q4_K_M}"
OLLAMA_HOST=${OLLAMA_HOST:-"http://ollama:11434"}
RETRIES=3
WAIT_SECONDS=5

echo "Starting Ollama initialization with models: ${MODELS[*]}"

# Wait for API to be ready
until curl -sSf "$OLLAMA_HOST/api/tags" >/dev/null; do
  echo "Waiting for Ollama API..."
  sleep 2
done

for MODEL in "${MODELS[@]}"; do
  echo "Initializing model: $MODEL"

  # Pull model with retries
  for ((i=1; i<=RETRIES; i++)); do
    if ollama list | grep -q "$MODEL"; then
      echo "Model $MODEL already exists"
      break
    else
      echo "Pulling model $MODEL (attempt $i/$RETRIES)..."
      if ollama pull "$MODEL"; then
        break
      else
        if [ "$i" -eq "$RETRIES" ]; then
          echo "Failed to pull model $MODEL after $RETRIES attempts"
          exit 1
        fi
        sleep "$WAIT_SECONDS"
      fi
    fi
  done

  # Prewarm model
  echo "Prewarming model $MODEL..."
  curl -sSf -X POST "$OLLAMA_HOST/api/generate" \
    -H "Content-Type: application/json" \
    -d '{
      "model": "'"$MODEL"'",
      "prompt": "ping",
      "stream": false
    }'

  # Verify loading
  if ! curl -sSf "$OLLAMA_HOST/api/ps" | grep -q "$MODEL"; then
    echo "Model $MODEL prewarm verification failed"
    exit 1
  fi

  echo "Model $MODEL initialized successfully"
done

echo "Ollama initialization completed successfully"
exit 0
