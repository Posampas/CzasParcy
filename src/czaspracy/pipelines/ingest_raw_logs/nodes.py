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

    print(rejected)
    return pd.DataFrame({'date' : dates, 'hour' : hour, 'event': event, 'place': place, 'person': person}), pd.DataFrame({'rejected' : rejected})
