import pandas as pd
import re
from datetime import time
from typing import List

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


def map_contact_points_to_sequence(logs: pd.DataFrame, mapping) -> pd.DataFrame:
    """
    input: pd.DataFrame containg the logs
    output: pd.DataFrame Data frame zawierajacy mozliwa sekwecje wejsc i
    wyjsc z obiektu.
    
    Mapuje Nazwy punktów kontatku, gdzie 1 to kontakt na
    bramce wysciowej a 0 to kontakt na bramce wewnatrz obiektu

    """
    if logs is None or mapping is None:
        raise RuntimeError('Input param can not be None')
    
    if not isinstance(logs, pd.DataFrame):
        raise RuntimeError('First input param should be dataFrame')
    if 'place' not in logs.columns:
        raise RuntimeError('Input frame should contain column: place')
    if not isinstance(mapping, dict):
         raise RuntimeError('Second input param should be dict')

    logs['sequence'] = logs['place'].map(mapping)
    
    return logs

def remove_Logs_With_Contact_Points_In(logs: pd.DataFrame, places_to_remove: List ):
    if not isinstance(logs, pd.DataFrame):
        raise RuntimeError('First input param should be dataFrame')
    if 'place' not in logs.columns:
        raise RuntimeError('Input frame should contain column: place')
    if not isinstance(places_to_remove, list):
        raise RuntimeError('places_to_remove parameter should be list of stirngs') 
    for place in places_to_remove:
        logs = logs[logs['place'] != place]
    return logs.reset_index()