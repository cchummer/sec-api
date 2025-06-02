#!/bin/bash

set -e

echo "Running Prefect database migrations..."
prefect server database upgrade --yes

echo "Waiting for Prefect API to be ready..."
until curl -sf "${PREFECT_API_URL:-http://prefect-server:4200/api}/health"; do
  sleep 2
done

echo "Prefect API is ready!"

prefect config set PREFECT_API_URL="${PREFECT_API_URL:-http://prefect-server:4200/api}"
prefect config set PREFECT_UI_ENABLED=false

if prefect work-pool inspect default-pool > /dev/null 2>&1; then
  echo "default-pool already exists."
else
  echo "Creating default-pool..."
  prefect work-pool create default-pool --type process
fi

if prefect deployment inspect "${PREFECT_FLOW_NAME}" --name daily-sec-pipeline > /dev/null 2>&1; then
  echo "Deployment already exists. Skipping deployment build/apply."
else
  echo "Creating deployment..."
  if prefect deployment build "${PREFECT_FLOW_NAME}" \
      --name daily-sec-pipeline \
      --infra process \
      --pool default-pool \
      --output sec_deploy.yaml; then
    prefect deployment apply sec_deploy.yaml
  else
    echo "Deployment build failed!"
  fi
fi

touch /tmp/prefect-init.done

echo "Prefect initialization complete. Entering idle state..."
tail -f /dev/null