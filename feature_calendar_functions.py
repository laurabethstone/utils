from datetime import datetime
from decimal import Decimal
# from math import ceil
from math import *  

now = datetime.now()

def get_current_period():
    """
    Get current year, quarter, month
    Returns:
        Year, quarter, month for current time
    """
    now_year = datetime.now().year
    now_mth = datetime.now().month
    now_qtr = ceil(now_mth / 3)

    return now_year, now_qtr, now_mth
