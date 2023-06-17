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

def calculate_time_in_office(logs: pd.DataFrame):
    if not isinstance(logs, pd.DataFrame):
        raise RuntimeError('First input param should be dataFrame')
    _throw_if_does_not_contain_column(logs.columns.to_list(), ['person','sequence','date','hour'])
    if len(logs) == 0:
        logs['work_time'] = None
        return logs
    logs['datetime'] = logs['date'] + " " + logs['hour']
    logs['datetime'] = pd.to_datetime(logs['datetime'])
    logs['work_time'] = logs['datetime'][len(logs) - 1] - logs['datetime'][0]
    return logs[:1]

def add_missing_entires(logs: pd.DataFrame):
    if not isinstance(logs, pd.DataFrame):
        raise RuntimeError('First input param should be dataFrame')
    _throw_if_does_not_contain_column(logs.columns.to_list(), ['person','sequence','date','hour','place'])
    persons = logs['person'].unique().tolist()
    person_frames = []
    for person in persons:
        person_frame = logs[logs['person'] == person]
        days = person_frame['date'].unique().tolist()
        person_days_frames = []
        for day in days:
            day_frame = person_frame[person_frame['date'] == day].copy(deep = True)
            person_days_frames.append(process_sequence(day_frame))
        person_frames.append(pd.concat(person_days_frames, axis=0, ignore_index= True))
    return pd.concat(person_frames, axis=0, ignore_index= True)
            
def process_sequence(logs: pd.DataFrame):
    if logs['sequence'].iloc[0] == 0:
        entry = logs[:1].copy(deep=True)
        entry['sequence'] = 1
        logs = pd.concat([entry,logs],axis=0, ignore_index= True)

    length = len(logs)
    if logs['sequence'].iloc[(length - 1)] == 0:
        entry = logs[length - 1:].copy(deep=True)
        entry['sequence'] = 1
        logs = pd.concat([logs,entry],axis=0, ignore_index= True) 

        
    inside = False
    to_insert = []
    for idx, log in logs.iterrows():
        current = log['sequence']
        if not inside and current == 0:
            insert = log.copy(deep=True)
            insert['sequence'] = 1
            to_insert.append((idx, insert))
            inside = True
        elif not inside and current == 1:
            inside = True
        elif inside and current == 1:
            inside = False

    # should be outside after exitg the loop
    if inside:
        entry = logs[length - 1:].copy(deep=True)
        entry['sequence'] = 1
        logs = pd.concat([logs,entry],axis=0, ignore_index= True) 

    for insert in to_insert:
            frame_dict = {}
            for column in logs.columns:
                frame_dict[column] = [insert[1][column]]
            before = logs[:insert[0]]
            after = logs[insert[0]:]
            logs =  pd.concat([before, pd.DataFrame(frame_dict) , after], axis=0, ignore_index=True)
    
    return logs.reindex()


def _throw_if_does_not_contain_column(frame_columns: List, required_columns: List) -> None:

    for column in required_columns:
        if column not in frame_columns:
            raise RuntimeError('Input should cotain column {}'.format(column))
        
seq = ['A','A','B','B']
input = pd.DataFrame({'person':seq, 'sequence':[0,0,0,1], 'date':seq, 'hour':seq,'place':seq})
result = add_missing_entires(input)
print(result)
       