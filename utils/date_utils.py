from datetime import datetime
import pytz


def get_current_date_yyyymmdd():
    sp_timezone = pytz.timezone('America/Sao_Paulo')
    current_date = datetime.now(sp_timezone)
    return current_date.strftime('%Y%m%d')
