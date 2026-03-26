import asyncio
import logging

from dotenv import load_dotenv
load_dotenv()

from camunda_orchestration_sdk import CamundaAsyncClient, WorkerConfig
from camunda_orchestration_sdk.runtime.job_worker import ConnectedJobContext, JobContext, JobFailure
from camunda_orchestration_sdk.models import MessagePublicationRequest
from camunda_orchestration_sdk.models.message_publication_request_variables import MessagePublicationRequestVariables

from services.credit_service import deduct_credit, get_customer_credit
from services.credit_card_service import charge_credit_card

logging.basicConfig(level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
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


async def handle_charge_credit_card(job: ConnectedJobContext) -> None:
    variables = job.variables.to_dict()
    card_number = variables["cardNumber"]
    cvc = variables["cvc"]
    expiry_date = variables["expiryDate"]
    open_amount = variables["openAmount"]

    try:
        charge_credit_card(card_number, cvc, expiry_date, open_amount)
        job.log.info(f"Credit card {card_number} charged for {open_amount}")
    except ValueError as e:
        # Raise JobFailure so the SDK handles the fail_job call internally.
        # retries=0 creates an incident immediately since a bad expiry date
        # won't be fixed by retrying.
        job.log.error(f"Credit card charge failed: {e}")
        raise JobFailure(message=str(e), retries=0)


async def handle_payment_invocation(job: ConnectedJobContext) -> None:
    job.log.info(f"Handling job type: {job.type_}")
    variables = job.variables.to_dict()
    order_id = variables["orderId"]
    await job.client.publish_message(
        data=MessagePublicationRequest(
            name="paymentRequestMessage",
            correlation_key=order_id,
            variables=MessagePublicationRequestVariables.from_dict(variables),
        )
    )


async def handle_payment_completion(job: ConnectedJobContext) -> None:
    job.log.info(f"Handling job type: {job.type_}")
    variables = job.variables.to_dict()
    order_id = variables["orderId"]
    await job.client.publish_message(
        data=MessagePublicationRequest(
            name="paymentCompletedMessage",
            correlation_key=order_id,
        )
    )


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

        client.create_job_worker(
            config=WorkerConfig(
                job_type="payment-invocation",
                job_timeout_milliseconds=30_000,
            ),
            callback=handle_payment_invocation,
        )

        client.create_job_worker(
            config=WorkerConfig(
                job_type="payment-completion",
                job_timeout_milliseconds=30_000,
            ),
            callback=handle_payment_completion,
        )

        logger.info("Workers started. Waiting for jobs...")
        await client.run_workers()


if __name__ == "__main__":
    asyncio.run(main())
