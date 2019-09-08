import datetime
def get_date():
    today = datetime.date.today()
    day = datetime.timedelta(days = 1)
    yesterday = today - day
    return yesterday