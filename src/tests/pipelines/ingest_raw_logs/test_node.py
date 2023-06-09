import pytest
import pandas as pd
from src.czaspracy.pipelines.ingest_raw_logs.nodes import convert_text_file_to_dataframe

def test():
    func = convert_text_file_to_dataframe
    assert func is not None


def test_should_throw_exception_when_input_is_not_string():
    input_var = 1
    with pytest.raises(RuntimeError) as err: 
        convert_text_file_to_dataframe(input_var)
        assert "Input param is not a str" in str(err)

def test_should_return_dataframe():
    test_string = "a"
    result, rejected = convert_text_file_to_dataframe(test_string)
    assert result is not None
    assert isinstance(result, pd.DataFrame)
    assert rejected is not None
    assert isinstance(result, pd.DataFrame)


def test_should_parse_line():
    test_string = '5 2023-05-31 17:01 - Za��czenie czuwania przez u�ytkownika          S:RECEPCJA,          U:TP_Rakowska_Z'
    result, rejected = convert_text_file_to_dataframe(test_string)

    assert len(result) == 1
    assert 'date' in result.columns
    assert '2023-05-31' == result['date'][0]
    assert 'Za��czenie czuwania przez u�ytkownika' not in result['date'][0]
    assert 'hour' in result.columns 
    assert '17:01' == result['hour'][0]
    assert 'event' in result.columns 
    assert 'Za��czenie czuwania przez u�ytkownika' == result['event'][0]
    assert 'place' in result.columns 
    assert 'S:RECEPCJA,' == result['place'][0]
    assert 'person' in result.columns 
    assert 'U:TP_Rakowska_Z' == result['person'][0] 

    test_string = '274 2023-05-31  7:09 - Dost�p u�ytkownika                             K:3 PI�TRO KLATKA    U:TP_StaszkiewiczP       '
    result, rejected= convert_text_file_to_dataframe(test_string)
    assert len(result) == 1
    assert 'date' in result.columns
    assert '2023-05-31' == result['date'][0]
    assert 'hour' in result.columns 
    assert '7:09' == result['hour'][0]
    assert 'event' in result.columns 
    assert 'Dost�p u�ytkownika' == result['event'][0]
    assert 'place' in result.columns 
    assert 'K:3 PI�TRO KLATKA' == result['place'][0]
    assert 'person' in result.columns 
    assert 'U:TP_StaszkiewiczP' == result['person'][0] 

def test_should_not_add_line():
    test_string = '4 2023-05-31 19:33 - Zbyt d�ugo otwarte drzwi                       M:PARTER KORYTARZ   '
    result, rejected = convert_text_file_to_dataframe(test_string)
    assert len(result) == 0
    assert len(rejected) == 1
    test_string = """
    Wydruk zdarze� obiektu: Technik_Polska_20230523  (INTEGRA 128 ver.1.17)
    Zdarzenia wybrane: w okresie: 01.05.2023 � 31.05.2023;
    Data wydruku: 01.06.2023 07:39:11
    
    Nr     Data    Godz.   Zdarzenie                                      Szczeg�y                                   
    ------------------------------------------------------------------------------------------------------------------------
    """
    print('/////////////////////////////////////////////////////////////////////////////////////////',rejected['rejected'])
    result, rejected = convert_text_file_to_dataframe(test_string)
    assert len(result) == 0
    assert len(rejected) == 5


def test_should_correctly_parse_multile_lines_with_correct_lines_and_wrong_lines():
    test_string = """
    Wydruk zdarze� obiektu: Technik_Polska_20230523  (INTEGRA 128 ver.1.17)
    Zdarzenia wybrane: w okresie: 01.05.2023 � 31.05.2023;
    Data wydruku: 01.06.2023 07:39:11
    
    Nr     Data    Godz.   Zdarzenie                                      Szczeg�y                                   
    ------------------------------------------------------------------------------------------------------------------------
    5 2023-05-31 17:01 - Za��czenie czuwania przez u�ytkownika          S:RECEPCJA,          U:TP_Rakowska_Z
    274 2023-05-31  7:09 - Dost�p u�ytkownika                             K:3 PI�TRO KLATKA    U:TP_StaszkiewiczP       
    4 2023-05-31 19:33 - Zbyt d�ugo otwarte drzwi                       M:PARTER KORYTARZ    
    """ 
    result, rejected = convert_text_file_to_dataframe(test_string)
    assert len(result) == 2
    assert len(rejected) == 6 