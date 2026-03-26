import asyncio
import logging

from dotenv import load_dotenv
load_dotenv()

from camunda_orchestration_sdk import CamundaAsyncClient, WorkerConfig
from camunda_orchestration_sdk.runtime.job_worker import ConnectedJobContext, JobContext, JobError, JobFailure
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
        # Invalid expiry date: trigger the BPMN error boundary event so the
        # process can follow the modelled error path (Charging failed).
        job.log.error(f"Credit card charge failed (BPMN error): {e}")
        raise JobError(error_code="creditCardChargeError", message=str(e))
    except Exception as e:
        # Unexpected error: create an incident in Operate for manual resolution.
        job.log.error(f"Credit card charge failed (incident): {e}")
        raise JobFailure(message=str(e), retries=0)


async def handle_payment_invocation(job: ConnectedJobContext) -> None:
    job.log.info(f"Handling job type: {job.type_}")
    variables = job.variables.to_dict()
    order_id = variables["orderId"]
    await job.client.publish_message(
        data=MessagePublicationRequest(
            name="paymentRequestMessage",
            correlation_key=order_id,
            time_to_live=60000,
            variables=MessagePublicationRequestVariables.from_dict(variables),
        )
    )

    job.log.info(f"Published order-processed message for order {order_id}")
    return {"status": "done"}


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


async def handle_payment_failure(job: ConnectedJobContext) -> None:
    job.log.info(f"Handling job type: {job.type_}")
    variables = job.variables.to_dict()
    order_id = variables["orderId"]
    await job.client.publish_message(
        data=MessagePublicationRequest(
            name="paymentFailedMessage",
            correlation_key=order_id,
        )
    )


async def handle_compensate_credit_deduction(job: JobContext) -> None:
    # Compensation handler: triggered when a BPMN error causes the credit
    # deduction to be rolled back. Log the compensation for traceability.
    variables = job.variables.to_dict()
    customer_id = variables.get("customerId", "unknown")
    order_total = variables.get("orderTotal", "unknown")
    job.log.info(
        f"Compensating credit deduction for customer {customer_id}: "
        f"restoring {order_total} to account"
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

        client.create_job_worker(
            config=WorkerConfig(
                job_type="payment-failure",
                job_timeout_milliseconds=30_000,
            ),
            callback=handle_payment_failure,
        )

        client.create_job_worker(
            config=WorkerConfig(
                job_type="compensate-credit-deduction",
                job_timeout_milliseconds=30_000,
            ),
            callback=handle_compensate_credit_deduction,
        )

        logger.info("Workers started. Waiting for jobs...")
        await client.run_workers()


if __name__ == "__main__":
    asyncio.run(main())
