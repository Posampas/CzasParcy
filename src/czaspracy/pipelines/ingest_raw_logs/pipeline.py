"""
This is a boilerplate pipeline 'ingest_raw_logs'
generated using Kedro 0.18.9
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import convert_text_file_to_dataframe, retain_persons_with_prefix, map_contact_points_to_sequence

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
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
        ),
        node(
            func= map_contact_points_to_sequence,
            inputs=["only_TP_emp", 'params:entrace_mapping'],
            outputs="contact_points_mapped",
            name="Zmapuj_odbicia_karty_do_0_i_1"
        )]
        ,
        inputs="raw_logs",
        outputs='contact_points_mapped'
    )
    