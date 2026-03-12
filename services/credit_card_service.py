# credit_card_service.py
import logging

logger = logging.getLogger(__name__)


def charge_credit_card(card_number: str, cvc: str, expiry_date: str, amount: float) -> None:
    """
    Simulates charging a credit card for the given amount.
    Raises ValueError if the expiry date format is invalid (expected: MM/YY, 5 chars).
    """
    if len(expiry_date) != 5:
        error_msg = f"Invalid expiry date '{expiry_date}': expected format MM/YY (5 characters)"
        logger.error(error_msg)
        raise ValueError(error_msg)

    print(f"Card {card_number}, with CVC {cvc} and expiry date {expiry_date} was charged for {amount}")
