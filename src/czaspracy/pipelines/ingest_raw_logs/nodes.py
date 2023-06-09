import pandas as pd
import re
from datetime import time

def convert_text_file_to_dataframe(contets : str):
    if not isinstance(contets, str):
        raise RuntimeError('Input param is not a str')
    contets = contets.splitlines()
    dates = []
    hour = []
    event = []
    place = []
    person = []
    rejected = []
    for line in contets:
        line = line.strip()
        if not line:
            continue
        splited = re.split(' - ',line)
        if len(splited) != 2:
            rejected.append(line)
            continue 
        date_time = re.split(r' +',splited[0])
        if len(date_time) != 3:
            rejected.append(line)
            continue
        date = date_time[1]
        time = date_time[2]
        
        details = re.split(r' {2,}', splited[1])
        if len(details) != 3:
            rejected.append(line)
            continue

        dates.append(date)
        hour.append(time)
        event.append(details[0])
        place.append(details[1])
        person.append(details[2])


    return pd.DataFrame({'date' : dates, 'hour' : hour, 'event': event, 'place': place, 'person': person}), pd.DataFrame({'rejected' : rejected})


def retain_persons_with_prefix(logs: pd.DataFrame, company_prefix: str):
    if not isinstance(logs, pd.DataFrame):
        raise RuntimeError('First input param should be dataFrame')
    if not isinstance(company_prefix, str):
        raise RuntimeError('Second input param should be string')
    if 'person' not in logs.columns:
        raise RuntimeError('Column person not in dataframe')
    
    logs = logs[logs['person'].str.contains(company_prefix)]
    return logs.reset_index()

test_data = ['A:Person1', 'B:Person1', 'C:Person1','A:Person1']
date = ['2022-02-22', '2022-02-22', '2022-02-22', '2022-02-22']
result = retain_persons_with_prefix(pd.DataFrame({'person' : test_data, 'date': date}), 'A:')
print(result)