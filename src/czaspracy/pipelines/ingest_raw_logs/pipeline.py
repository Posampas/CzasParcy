"""
This is a boilerplate pipeline 'ingest_raw_logs'
generated using Kedro 0.18.9
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import convert_text_file_to_dataframe

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func= convert_text_file_to_dataframe,
            inputs="raw_logs",
            outputs=["parsed_logs",'rejected_logs'],
            name="parse_raw_log_file",
        )],

        inputs="raw_logs",
        outputs=["parsed_logs",'rejected_logs']
    )
    