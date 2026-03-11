# credit_service.py

def get_customer_credit(customer_id: str) -> str:
    """
    Returns the customer's available credit amount based on their ID.
    """
    return customer_id[-2:]


def deduct_credit(customer_id: str, amount: float) -> float:
    """
    Deducts the customer's available credit from the total amount due.
    """
    open_amount = amount - float(get_customer_credit(customer_id))
    return open_amount