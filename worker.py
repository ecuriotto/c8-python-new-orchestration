import asyncio
import logging

from camunda_orchestration_sdk import CamundaAsyncClient, WorkerConfig
from camunda_orchestration_sdk.runtime.job_worker import JobContext

from services.credit_service import deduct_credit, get_customer_credit
from services.credit_card_service import charge_credit_card

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def handle_credit_deduction(job: JobContext) -> dict:
    variables = job.variables.to_dict()
    customer_id = variables["customerId"]
    order_total = variables["orderTotal"]

    customer_credit = get_customer_credit(customer_id)
    job.log.info(f"Customer credit for {customer_id}: {customer_credit}")

    open_amount = deduct_credit(customer_id, order_total)
    job.log.info(f"Open amount after credit deduction: {open_amount}")

    return {"openAmount": open_amount}


async def handle_charge_credit_card(job: JobContext) -> None:
    variables = job.variables.to_dict()
    card_number = variables["cardNumber"]
    cvc = variables["cvc"]
    expiry_date = variables["expiryDate"]
    open_amount = variables["openAmount"]

    charge_credit_card(card_number, cvc, expiry_date, open_amount)
    job.log.info(f"Credit card {card_number} charged for {open_amount}")


async def main():
    async with CamundaAsyncClient() as client:
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
