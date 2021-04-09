from datetime import datetime

def to_days_since_epoch(date: str):
    print (date)
    epoch = datetime.utcfromtimestamp(0)
    date_obj = datetime.strptime(date, '%Y-%m-%d')
    return ((date_obj - epoch).days)

print(to_days_since_epoch("2000-01-01"))
print(to_days_since_epoch("2000-12-31"))


print(to_days_since_epoch("2000-06-01"))
print(to_days_since_epoch("2002-06-01"))
