"""
This is a boilerplate pipeline 'ingest_raw_logs'
generated using Kedro 0.18.9
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import convert_text_file_to_dataframe, retain_persons_with_prefix

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func= convert_text_file_to_dataframe,
            inputs="raw_logs",
            outputs=["parsed_logs",'rejected_logs'],
            name="parse_raw_log_file",
        ),
        node(
            func= retain_persons_with_prefix,
            inputs=["parsed_logs", "params:technik_polska_perfix"],
            outputs="only_TP_emp",
            name="retain_only_technik_empl",
        )],

        inputs="raw_logs",
        outputs='only_TP_emp'
    )
    