from datetime import datetime, timedelta
from decimal import Decimal
import random
from typing import List, Tuple

def generate_data(start_id: int, end_id: int) -> List[Tuple[int, datetime.date, Decimal]]:
    """
    Generate sample data with IDs ranging from start_id to end_id.

    Args:
        start_id (int): The starting ID for the range.
        end_id (int): The ending ID for the range.

    Returns:
        List[Tuple[int, datetime.date, Decimal]]: A list of tuples containing the generated data.
            Each tuple consists of (id, transaction_date, amount).
    """
    today = datetime.now()
    data = []
    for i in range(start_id, end_id + 1):
        id = i
        transaction_date = today - timedelta(days=random.randint(0, 365))
        amount = Decimal(round(random.uniform(10.0, 1000.0), 2))
        data.append((id, transaction_date, amount))
    return data