"""TO execute:
# dagster pipeline execute -f cereal_pipeline_parameterized.py -c input_env_for_parametized.yaml
"""
import csv
import os
os.environ['PYTHONLEGACYWINDOWSSTDIO']='enable'

from dagster import execute_pipeline , pipeline , solid


@solid
def read_csv(context, csv_path):
    csv_path=os.path.join(os.path.dirname(__file__),csv_path)
    with open(csv_path,'r') as fd:
        lines=[i for i in csv.DictReader(fd)]
    
    context.log.info('No of line  : {n_lines}'.format(n_lines=len(lines)))

    return lines

@solid
def sort_by_calories(context,cereals):
    sorted_cereals=sorted(cereals,key=lambda cereal:cereal['calories'])

    context.log.info(
        'Least caloric cereal : {least_calorific}'.format(least_calorific=sorted_cereals[0]['name'])
    )

    context.log.info(
        'Most caloric cereal: {most_caloric}'.format(
            most_caloric=sorted_cereals[-1]['name']
        )
    )
    return {
        'least_caloric': sorted_cereals[0],
        'most_caloric': sorted_cereals[-1]
    }

@pipeline
def input_pipeline():
    sort_by_calories(read_csv())


run_config={
    'solids':{
        'read_csv':{'inputs':{'csv_path':{'value':'cereal.csv'}}}
    }
}

result=execute_pipeline(input_pipeline, run_config=run_config)
assert result.success