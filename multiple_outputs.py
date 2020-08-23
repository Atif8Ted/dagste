
import csv
import os
os.environ['PYTHONLEGACYWINDOWSSTDIO']='enable'

from dagster import (
    Bool, String,Field,Int,execute_pipeline,pipeline,solid,OutputDefinition,Output
)

@solid
def read_csv(context, csv_path):
    csv_path=os.path.join(os.path.dirname(__file__),csv_path)
    with open(csv_path,'r') as fd:
        lines=[i for i in csv.DictReader(fd)]
    
    context.log.info('No of line  : {n_lines}'.format(n_lines=len(lines)))

    return lines


@solid (
config_schema={
    'process_hot': Field(Bool,is_required=False,default=True),
    'process_cold': Field(Bool,is_required=False, default=True)
},
output_defs=[
    OutputDefinition(
        name='hot_cereals',dagster_type=DataFrame,is_required=False
    ),
    OutputDefinition(
        name='cold_cereals',dagster_type=DataFrame,is_required=False
    ),
]
)
def split_cereals(context,cereals):
    if context.solid_config['process_hot']:
        hot_cereals=[cereal for cereal in cereals if cereal['type']=='H']
        yield Output(hot_cereals,'hot_cereals')
    
    if context.solid_config['process_cold']:
        cold_cereals=[cereal for cereal in cereals if cereal['type']=='C']
        yield  Output(cold_cereals,'cold_cereals')


