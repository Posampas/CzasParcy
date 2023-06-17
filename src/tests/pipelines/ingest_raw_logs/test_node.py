import pytest
import pandas as pd
from src.czaspracy.pipelines.ingest_raw_logs.nodes import convert_text_file_to_dataframe
from src.czaspracy.pipelines.ingest_raw_logs.nodes import retain_persons_with_prefix
from src.czaspracy.pipelines.ingest_raw_logs.nodes import map_contact_points_to_sequence
from src.czaspracy.pipelines.ingest_raw_logs.nodes import remove_Logs_With_Contact_Points_In
from src.czaspracy.pipelines.ingest_raw_logs.nodes import calculate_time_in_office
from src.czaspracy.pipelines.ingest_raw_logs.nodes import add_missing_entires

class TestConvertTextFileToDataFrame:
    def test(self):
        func = convert_text_file_to_dataframe
        assert func is not None


    def test_should_throw_exception_when_input_is_not_string(self):
        input_var = 1
        with pytest.raises(RuntimeError) as err: 
            convert_text_file_to_dataframe(input_var)
            assert "Input param is not a str" in str(err)

    def test_should_return_dataframe(self):
        test_string = "a"
        result, rejected = convert_text_file_to_dataframe(test_string)
        assert result is not None
        assert isinstance(result, pd.DataFrame)
        assert rejected is not None
        assert isinstance(result, pd.DataFrame)


    def test_should_parse_line(self):
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

    def test_should_not_add_line(self):
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


    def test_should_correctly_parse_multile_lines_with_correct_lines_and_wrong_lines(self):
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

class TestRemoveNotCompanyWorkesLogs():

    def test_can_import(self):
        func = retain_persons_with_prefix
        assert func is not None
    
    def test_should_throw_when_not_dataframe_and_string_passed(self):
        with pytest.raises(RuntimeError) as err: 
            retain_persons_with_prefix(1, "df")
            assert "First input param should be dataFrame" in str(err) 

        with pytest.raises(RuntimeError) as err: 
            retain_persons_with_prefix(pd.DataFrame(), 1)
            assert "Second input param should be string" in str(err)

    def test_should_not_return_None(self):
        result = retain_persons_with_prefix(pd.DataFrame({'person':['ad']}), "TP")
        assert result is not None    

    def test_should_return_dataFrame(self):
        result = retain_persons_with_prefix(pd.DataFrame({'person':['aa']}), "TP")
        assert isinstance(result, pd.DataFrame)

    def test_should_remove_logs_where_person_do_not_have_prefix(self):
        test_data = ['A:Person1', 'B:Person1', 'C:Person1','A:Person1']
        date = ['2022-02-22', '2022-02-22', '2022-02-22', '2022-02-22']
        result = retain_persons_with_prefix(pd.DataFrame({'person' : test_data, 'date': date}), 'A:')
        assert len(result) == 2
        assert result['person'][0] == 'A:Person1' 
        assert result['person'][1] == 'A:Person1' 

        test_data = ['A:Person1', 'A:Person2', 'C:Person1','A:Person1']
        result = retain_persons_with_prefix(pd.DataFrame({'person' : test_data, 'date':date}), 'A:')
        assert len(result) == 3
        assert result['person'][0] == 'A:Person1' 
        assert result['person'][1] == 'A:Person2'  
        assert result['person'][2] == 'A:Person1'

class TestMapContactPointsFunction():


        
    def test_can_inport(self):
        func = map_contact_points_to_sequence
        assert  func is not None

    def test_has_dataframe_as_an_input_and_dictiorny_for_mapping(self):
        with pytest.raises(RuntimeError) as err: 
            map_contact_points_to_sequence("df", {'234':'234'})
            assert "First input param should be dataFrame" in str(err)

    def test_has_second_param_is_an_mapping_dict(self):      
        with pytest.raises(RuntimeError) as err: 
            map_contact_points_to_sequence(pd.DataFrame(), "dd")
            assert "Second input param should be dict" in str(err)
    
    def test_should_not_return_None(self):
        result = map_contact_points_to_sequence(pd.DataFrame({'person': [] , 'place': []}) ,{'asdf': 'asdf'})
        assert result is not None

    def test_raise_erors_if_input_data_frame_not_have_columns(self):
        input_frame = pd.DataFrame() 
         # Should contain colum:  'place'
        with pytest.raises(RuntimeError) as err:
            result = map_contact_points_to_sequence(input_frame, {})
            assert 'Input frame should contain column: place' in str(err)
    

    def test_assert_retun_type_isdataframe(self):
        correct_input_frame = pd.DataFrame({'place':['A', 'B']})
        result = map_contact_points_to_sequence(correct_input_frame, {})

        assert isinstance(result,pd.DataFrame)
    
    def test_result_frame_shoul_contain_colum_segence(self):
        correct_input_frame = pd.DataFrame({'place':['A', 'B']})
        result = map_contact_points_to_sequence(correct_input_frame , {})
        assert isinstance(result,pd.DataFrame)
        assert 'sequence' in result.columns

    def test_should_correcy_map_contact_points(self):
        mapping = {'A' : 1, 'B' : 0}
        correct_input_frame = pd.DataFrame({'place':['A', 'B']})
        result = map_contact_points_to_sequence(correct_input_frame, mapping)
 
        assert result['sequence'][0] == 1
        assert result['sequence'][1] == 0
        
        correct_input_frame = pd.DataFrame({'place':['A', 'A']})
        result = map_contact_points_to_sequence(correct_input_frame, mapping)
 
        assert result['sequence'][0] == 1
        assert result['sequence'][1] == 1

        correct_input_frame = pd.DataFrame({'place':['B', 'B']})
        result = map_contact_points_to_sequence(correct_input_frame, mapping)
 
        assert result['sequence'][0] == 0
        assert result['sequence'][1] == 0

class TestRemoveNotCorrectContactPoints:

    def test_can_import(slef):
        func = remove_Logs_With_Contact_Points_In
        assert func is not None
    
    def test_should_throw_when_input_is_not_dataframe(slef):


        with pytest.raises(RuntimeError) as err:
            remove_Logs_With_Contact_Points_In('', '')
            assert 'First input param should be dataFrame' in str(err)
        
    def test_should_throw_when_frame_do_not_contain_column_place(self):
        with pytest.raises(RuntimeError) as err:
            remove_Logs_With_Contact_Points_In(pd.DataFrame(), ['A'])
            assert ('Input frame should contain column: place') in str(err)

    def test_take_list_of_places_to_remove_as_parameter(self):
        input = pd.DataFrame({'place':['A']})
        remove_Logs_With_Contact_Points_In(input, ['A']) 

        with pytest.raises(RuntimeError) as err:
            remove_Logs_With_Contact_Points_In(input, 'A')
            assert ('places_to_remove parameter should be list of stirngs') in str(err)

    def test_shoud_not_return_None(self):
        input = pd.DataFrame({'place':['A']})
        result = remove_Logs_With_Contact_Points_In(input, ['A'])
        assert result is not None

    def test_should_return_dataframe(slef):
        input = pd.DataFrame({'place':['A']})
        result = remove_Logs_With_Contact_Points_In(input, ['A'])
        assert result is not None
        assert isinstance(result, pd.DataFrame)
    
    def test_should_remove_places_from_list(slef):
        input = pd.DataFrame({'place':['A']})
        result = remove_Logs_With_Contact_Points_In(input, ['A'])
        assert result is not None
        assert len(result ) == 0


        input = pd.DataFrame({'place':['A', 'B']})
        result = remove_Logs_With_Contact_Points_In(input, ['A'])
        assert result is not None
        assert len(result ) == 1
        assert result['place'][0] == 'B' 

        input = pd.DataFrame({'place':['A', 'B', 'C','D','D']})
        result = remove_Logs_With_Contact_Points_In(input, ['A','D'])
        assert result is not None
        assert len(result ) == 2
        assert result['place'].tolist() == ['B','C']

class TestCalcuateTimeForOneDay:
    def test_should_import(self):
        func = calculate_time_in_office
        assert func is not None
    
    def test_should_take_dataframe_as_parameter(self):

        with pytest.raises(RuntimeError) as err:
            calculate_time_in_office("sadf")
            assert 'First input param should be dataFrame' in str(err) 

    def test_input_should_contain_columns(self):
        input = pd.DataFrame({'person':[], 'sequence':[], 'date':[], 'hour':[]})

        with pytest.raises(RuntimeError) as err:
            calculate_time_in_office(pd.DataFrame({ 'sequence':[], 'date':[], 'hour':[]}))
            assert 'Input should cotain column person' in str(err) 

        with pytest.raises(RuntimeError) as err:
            calculate_time_in_office(pd.DataFrame({'person':[],  'date':[], 'hour':[]}))
            assert 'Input should cotain column person' in str(err) 

        with pytest.raises(RuntimeError) as err:
            calculate_time_in_office(pd.DataFrame({'person':[], 'sequence':[],  'hour':[]}))
            assert 'Input should cotain column person' in str(err)

        with pytest.raises(RuntimeError) as err:
            calculate_time_in_office(pd.DataFrame({'person':[], 'sequence':[], 'date':[], }))
            assert 'Input should cotain column person' in str(err) 

    def test_should_return_dataframe(self):
        input = pd.DataFrame({'person':[], 'sequence':[], 'date':[], 'hour':[]})
        result = calculate_time_in_office(input)

        assert isinstance(result,pd.DataFrame)

    def test_should_return_empty_dataframe_if_input_data_is_empty(self):
        input = pd.DataFrame({'person':[], 'sequence':[], 'date':[], 'hour':[]})
        result = calculate_time_in_office(input)

        assert isinstance(result,pd.DataFrame)
        assert len(result) == 0 

    def test_should_contain_column_work_time(self):
        input = pd.DataFrame({'person':[], 'sequence':[], 'date':[], 'hour':[]})
        result = calculate_time_in_office(input)
        assert 'work_time' in result.columns.to_list()

    def test_should_correct_value_if_sequence_(self):
        input = pd.DataFrame({'person':['A','A'], 'sequence':[1,1], 'date':['2023-01-01','2023-01-01'], 'hour':['7:00','8:00']}) 

        result = calculate_time_in_office(input)
        assert result['person'][0] == 'A'
        assert isinstance(result['datetime'][0],pd.Timestamp)
        assert result['date'][0] == '2023-01-01'
        assert result['datetime'][0] == pd.Timestamp('2023-01-01 7:00:00')
        assert result['work_time'][0] == pd.Timedelta(hours=1)
        assert len(result) == 1

        input = pd.DataFrame({'person':['A','A','A'], 'sequence':[1,0,1], 'date':['2023-01-01','2023-01-01','2023-01-01'], 'hour':['7:00','8:00','9:00']})  
        result = calculate_time_in_office(input)
        assert result['work_time'][0] == pd.Timedelta(hours=2)
        assert len(result) == 1

        # input = pd.DataFrame({'person':['A','A','A','A','A'], 'sequence':[1,0,1,1,1], 'date':['2023-01-01','2023-01-01','2023-01-01','2023-01-01','2023-01-01'], 'hour':['7:00','8:00','9:00','10:00','11:00' ]})  
        # result = calculate_time_in_office(input)
        # assert result['work_time'][0] == pd.Timedelta(hours=3)
        # assert len(result) == 1

class TestAppendMissingEntryPoints():

    

    def test_can_import(self):
        func = add_missing_entires
        assert func is not None

    def test_should_throw_error_if_dataframe_is_not_an_input(self):
        with pytest.raises(RuntimeError) as err:
            add_missing_entires("sadf")
            assert 'First input param should be dataFrame' in str(err) 
    
    def test_should_throw_when_columns_not_present(self):
        input = pd.DataFrame({'person':['A','A'], 'sequence':[1,1], 'date':['2023-01-01','2023-01-01'], 'place':['A','A'], 'hour':['1:00','2:00']}) 

        with pytest.raises(RuntimeError) as err:
            add_missing_entires(input.drop(axis=1 , columns='person'))
            assert 'First input param should be dataFrame' in str(err)  
        with pytest.raises(RuntimeError) as err:
            add_missing_entires(input.drop(axis=1 , columns='date'))
            assert 'First input param should be dataFrame' in str(err)  
        with pytest.raises(RuntimeError) as err:
            add_missing_entires(input.drop(axis=1 , columns='sequence'))
            assert 'First input param should be dataFrame' in str(err) 
        with pytest.raises(RuntimeError) as err:
            add_missing_entires(input.drop(axis=1 , columns='place'))
            assert 'First input param should be dataFrame' in str(err) 
        with pytest.raises(RuntimeError) as err:
            add_missing_entires(input.drop(axis=1 , columns='hour'))
            assert 'First input param should be dataFrame' in str(err) 
    
    
    def test_should_return_dataframe(self):

        input = pd.DataFrame({'person':['A','A'], 'sequence':[1,1], 'date':['2023-01-01','2023-01-01'], 'place':['A','A'], 'hour':['1:00','2:00']}) 
        result = add_missing_entires(input)
        assert isinstance(result, pd.DataFrame)

    def test_should_retrun_correct_entry_point(self):

        # should not change enything 
        input = pd.DataFrame({'person':['A','A'], 'sequence':[1,1], 'date':['2023-01-01','2023-01-01'], 'hour':['7:16','7:00'],'place':['A','A']})
        result = add_missing_entires(input)
        assert result['sequence'].tolist() == [1,1]

    def test_should_add_log_entry_in_the_begging(self):
        # should add entry point in the begiging
        input = pd.DataFrame({'person':['A','A'], 'sequence':[0,1], 'date':['2023-01-01','2023-01-01'], 'hour':['7:16','7:00'],'place':['A','A']})
        result = add_missing_entires(input)
        assert isinstance(result, pd.DataFrame)
        assert result.columns.to_list() == input.columns.tolist()
        assert len(result) == 3
        assert result['sequence'].tolist() == [1,0,1]

    def test_should_add_log_entry_in_the_end(self):
        # should add entry point in the end
        input = pd.DataFrame({'person':['A','A'], 'sequence':[1,0], 'date':['2023-01-01','2023-01-01'], 'hour':['7:16','7:00'],'place':['A','A']})
        result = add_missing_entires(input)
        print(result)
        assert len(result) == 3
        assert result['sequence'].tolist() == [1,0,1]        

    def test_should_add_log_entry_in_the_start_and_begging(self):
        input = pd.DataFrame({'person':['A','A'], 'sequence':[0,0], 'date':['2023-01-01','2023-01-01'], 'hour':['7:16','7:00'],'place':['A','A']})
        result = add_missing_entires(input)
        print(result)
        assert len(result) == 4
        assert result['sequence'].tolist() == [1,0,0,1]

        input = pd.DataFrame({'person':['A'], 'sequence':[0], 'date':['2023-01-01'], 'hour':['7:16'],'place':['A']})
        result = add_missing_entires(input)
        assert result['sequence'].tolist() == [1,0,1]       

    def test_should_add(self):
        input = _create_test_logs(4)
        input['sequence'] = [0,1,0,0]
        result = add_missing_entires(input)
        print(result['sequence'].to_list())
        assert result['sequence'].tolist() == [1,0,1,1,0,0,1] 

        input['sequence'] = [0,0,1,0]
        result = add_missing_entires(input)
        print(result['sequence'].to_list())
        assert result['sequence'].tolist() == [1,0,0,1,1,0,1] 

        input['sequence'] = [0,0,1,1]
        result = add_missing_entires(input)
        print(result['sequence'].to_list())
        assert result['sequence'].tolist() == [1,0,0,1,1,1] 
    
        input['sequence'] = [0,1,0,1]
        result = add_missing_entires(input)
        print(result['sequence'].tolist() )
        assert result['sequence'].tolist() == [1,0,1,1,0,1] 
        
        input['sequence'] = [1,1,1,0]
        result = add_missing_entires(input)
        print(result['sequence'].tolist() )
        assert result['sequence'].tolist() == [1,1,1,0,1] 

        input['sequence'] = [1,1,0,0]
        result = add_missing_entires(input)
        print(result['sequence'].tolist() )
        assert result['sequence'].tolist() == [1,1,1,0,0,1]


    def test_should_check_sequences_for_perons_for_days(self):
        input = _create_test_logs(2, person_name = 'A')
        input['sequence'] = [1,1] 
        input = pd.concat( [ input, _create_test_logs(2, person_name = 'A')], axis=0, ignore_index = True )
        input['sequence'] = [1,1,1,1]

        result = add_missing_entires(input)
        assert result['sequence'].tolist() == [1,1,1,1]

        
        input = _create_test_logs(2, person_name = 'A')
        input = pd.concat( [ input, _create_test_logs(2, person_name = 'B')], axis=0, ignore_index = True )
        input['sequence'] = [0,0,0,1]


        result = add_missing_entires(input)
        assert result['sequence'].tolist() == [1,0,0,1,1,0,1]

    


def _create_test_logs(length, person_name = 'A'):
    places  = ['place1' for i in range(length)]
    person  = [person_name for i in range(length)]
    date  = ['2023-12-1' for i in range(length)]
    hour  = ['8:0' for i in range(length)]

    return pd.DataFrame({'person':person, 'date': date, 'hour':hour,'place':places})





    


