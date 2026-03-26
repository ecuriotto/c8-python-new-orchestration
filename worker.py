import asyncio
import logging

from dotenv import load_dotenv
load_dotenv()

from camunda_orchestration_sdk import CamundaAsyncClient, WorkerConfig
from camunda_orchestration_sdk.runtime.job_worker import JobContext

logging.basicConfig(level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


async def handle_credit_deduction(job: JobContext) -> None:
    job.log.info(f"Handling credit deduction job {job.job_key}")


async def handle_charge_credit_card(job: JobContext) -> None:
    job.log.info(f"Handling credit card charging job {job.job_key}")


async def main():
    async with CamundaAsyncClient(logger=logger) as client:
        client.create_job_worker(
            config=WorkerConfig(
                job_type="credit-deduction",
                job_timeout_milliseconds=30_000,
            ),
            callback=handle_credit_deduction,
        )

        client.create_job_worker(
            config=WorkerConfig(
                job_type="credit-card-charging",
                job_timeout_milliseconds=30_000,
            ),
            callback=handle_charge_credit_card,
        )

        logger.info("Workers started. Waiting for jobs...")
        await client.run_workers()


if __name__ == "__main__":
    asyncio.run(main())
