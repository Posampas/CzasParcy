import pytest
from src.czaspracy.utils.state_machine import StateMachine, State
import pandas as pd


class TestStateMachine():

    def test_can_create(self):
        machine = StateMachine()
        assert machine is not None
    
    def test_has_method_calcuate_time_in_office(self):
        machine = StateMachine()
        assert machine.calcuate_time_in_office is not None
        assert 'calcuate_time_in_office' in  dir(StateMachine)

    def test_input_is_a_dataFrame_containg(self):
        input_frame = pd.DataFrame()
        machine = StateMachine()
        machine.calcuate_time_in_office(input_frame)
    

class TestState():
    def test_can_create(self):
        state = State()
        assert state is not None
        
