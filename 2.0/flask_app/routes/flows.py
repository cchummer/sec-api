import logging
import asyncio
from flask import Blueprint, jsonify


import config.settings as settings
from config.log_config import config_logging
config_logging('web') ## TESTING

from prefect.client import get_client

flows_bp = Blueprint('flows', __name__, url_prefix='/api/flows')

# Route to trigger a Prefect deployment
@flows_bp.route('/ingest/<date_str>')
def ingest_by_date(date_str):
    logging.info(f'/ingest route is processing request with target date: {date_str}. Attempting to run prefect deployment.')

    async def trigger_flow():
        async with get_client() as client:
            # Look up the deployment
            deployment = await client.read_deployment_by_name(
                name=f"{settings.PREFECT_FLOW_NAME}/daily-sec-pipeline"
            )
            # Trigger flow run 
            flow_run = await client.create_flow_run_from_deployment(
                deployment_id=deployment.id,
                parameters={"target_date": date_str},
            )
            return flow_run.id

    try:
        flow_run_id = asyncio.run(trigger_flow())
        logging.info(f"Flow submitted with run ID: {flow_run_id}")
        return jsonify({"status": "submitted", "flow_run_id": flow_run_id}), 202
    except Exception as e:
        logging.exception("Failed to trigger flow:")
        return jsonify({"error": str(e)}), 500

# Route to check the status of a Prefect flow run
@flows_bp.route('/job/<flow_run_id>')
def check_prefect_job(flow_run_id):
    logging.info(f'/job route is checking status for flow run ID: {flow_run_id}')

    async def get_status():
        async with get_client() as client:
            flow_run = await client.read_flow_run(flow_run_id)
            return {
                "status": flow_run.state.name,
                "created": str(flow_run.created),
                "start_time": str(flow_run.start_time),
                "end_time": str(flow_run.end_time),
                "parameters": flow_run.parameters,
            }

    try:
        result = asyncio.run(get_status())
        return jsonify(result)
    except Exception as e:
        logging.exception("Failed to fetch job status:")
        return jsonify({"error": str(e)}), 500