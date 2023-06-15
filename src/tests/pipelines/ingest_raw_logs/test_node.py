import pytest
import pandas as pd
from src.czaspracy.pipelines.ingest_raw_logs.nodes import convert_text_file_to_dataframe
from src.czaspracy.pipelines.ingest_raw_logs.nodes import retain_persons_with_prefix
from src.czaspracy.pipelines.ingest_raw_logs.nodes import map_contact_points_to_sequence
from src.czaspracy.pipelines.ingest_raw_logs.nodes import remove_Logs_With_Contact_Points_In

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
    
    def test_should_remove_plaves_from_list(slef):
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