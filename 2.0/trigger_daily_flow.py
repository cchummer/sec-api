import asyncio
import logging
from datetime import datetime, timedelta
from prefect.client import get_client
import config.settings as settings
from config.log_config import config_logging

async def trigger_yesterday_flow():
    yesterday = datetime.now() - timedelta(days=1)
    if yesterday.weekday() < 5:  # Only run on weekdays
        logging.info(f'Triggering ingestion flow for date: {yesterday.date()}, via scheduler.')

        async with get_client() as client:
            deployment = await client.read_deployment_by_name(
                name=f'{settings.PREFECT_FLOW_NAME}/daily-sec-pipeline'
            )
            flow_run = await client.create_flow_run_from_deployment(
                deployment_id=deployment.id,
                parameters={'target_date': yesterday.strftime('%Y-%m-%d')}
            )
            logging.info(f'Submitted flow run for {yesterday.date()}: Flow run ID {flow_run.id}')


if __name__ == '__main__':
    config_logging('scheduler')
    asyncio.run(trigger_yesterday_flow())
