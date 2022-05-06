from datetime import date, timedelta

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


def previous_weekday():
    # today weekday    = 0, 2, 3, 4, 5, 6
    # previous weekday = 4, 1, 2, 3, 4, 4
    today_weekday = date.today().weekday()
    if today_weekday == 0:
        return date.today() - timedelta(days=3)
    elif today_weekday == 6:
        return date.today() - timedelta(days=2)
    else:
        return date.today() - timedelta(days=1)
