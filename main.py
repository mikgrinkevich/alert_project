import pandas as pd
import schedule
import time
from send_mail import send_mail

def process_and_clean_data():
    df = pd.read_csv('data/data.csv', parse_dates=['23'])
    df.columns.values[0:24] =['error_code', 'error_message', \
        'severity', 'log_location', 'mode', 'model', 'graphics', \
        'session_id', 'sdkv', 'test_mode', 'flow_id', 'flow_type', \
        'sdk_date', 'publisher_id', 'game_id', 'bundle_id', 'appv', \
        'language', 'os', 'adv_id', 'gdpr', 'ccpa', 'country_code', 'date']
    df['datetime'] = pd.to_datetime(df['date'], unit='s')
    df = df.drop(['date'], axis=1)
    df['minute'] = df.datetime.dt.strftime('%d/%m/%y %H:%M')
    df['hour'] = df.datetime.dt.strftime('%d/%m/%y %H')
    df['second'] = df.datetime.dt.strftime('%d/%m/%y %H:%M:%S')
    df['day'] = df.datetime.dt.strftime('%d/%m/%y')
    df['month'] = df.datetime.dt.strftime('%m/%y')
    df.to_csv('data/data_processed.csv')
    
def read_df():
    df = pd.read_csv('data/data_processed.csv')
    return df

def flexible(grouping: list, rows_to_return: int, alert_if_faults_more_than: int):
    df = read_df()
    grouped_df = df.groupby(grouping).count()
    faults_by_bundle_within_hour = grouped_df.head(rows_to_return)['error_code'].to_dict()
    return any(x > alert_if_faults_more_than for x in faults_by_bundle_within_hour.values())

def notify(grouping, rows_to_return, alert_if_faults_more_than):
    rule = flexible(grouping, rows_to_return, alert_if_faults_more_than)
    if rule == True:
        send_mail(f'This alert was sent cause number of faults more than {alert_if_faults_more_than}\
            first - {rows_to_return} analyzed, grouping attributes - {grouping}')

def scheduler(period_of_time_in_minutes: int, grouping: list, rows_to_return: int, alert_if_faults_more_than: int):
    schedule.every(period_of_time_in_minutes).minute.do(notify, grouping, rows_to_return, alert_if_faults_more_than)

# rule number 1
scheduler(1, ['minute'], 1, 10)
# rule number 2
scheduler(1, ['hour', 'bundle_id'], 4, 10)

while True:
    schedule.run_pending()
    time.sleep(1)