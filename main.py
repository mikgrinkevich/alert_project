import pandas as pd
import schedule
import time
from send_mail import send_mail

def process_and_clean_data():
    df = pd.read_csv('data/data.csv', parse_dates=['23'])
    df['datetime'] = pd.to_datetime(df['23'], unit='s')
    df = df.drop(['23'], axis=1)
    df['datetime_minutes'] = df.datetime.dt.strftime('%d/%m/%y %H:%M')
    df['datetime_hours'] = df.datetime.dt.strftime('%d/%m/%y %H')
    df.to_csv('data/data_processed.csv')
    
def read_df():
    df = pd.read_csv('data/data_processed.csv')
    return df
    
df = read_df()
#reading a dataframe

def ten_faults_less_than_a_minute(df=df):
    # output - returns True if there are more than 10 faults within a 15 minutes interval
    group_by_minute = df.groupby(df['datetime_minutes']).count()
    first_fifteen_minutes_dict = group_by_minute.head(15)['datetime_hours'].to_dict()
    return any(x > 10 for x in first_fifteen_minutes_dict.values())

def ten_faults_for_a_bundle_wihin_hour(df=df):
    # output - returns True if there are more than 10 faults for bundle_id within an hour
    group_by_hour = df.groupby(['datetime_hours', '15']).count()
    faults_by_bundle_within_hour = group_by_hour.head(3)['0'].to_dict()
    return any(x > 10 for x in faults_by_bundle_within_hour.values())

def notify():
    rule1 = ten_faults_less_than_a_minute()
    rule2 = ten_faults_for_a_bundle_wihin_hour()
    if rule1 == True:
        send_mail('1')
    if rule2 == True:
        send_mail('2')

def scheduler(rule1, rule2):
    schedule.every(1).minutes.do(rule1)
    schedule.every(40).seconds.do(rule2)
    # schedule.every().hour.do(rule2)
    notify()
    print('&')

schedule.every(1).minutes.do(scheduler, ten_faults_less_than_a_minute, ten_faults_for_a_bundle_wihin_hour)

while True:
    schedule.run_pending()
    time.sleep(10)

# if __name__ == '__main__':
    # print(ten_faults_less_than_a_minute(process_and_clean_data()))
    # print(ten_faults_for_a_bundle_wihin_hour(process_and_clean_data()))