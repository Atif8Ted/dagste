import csv
import os
os.environ['PYTHONLEGACYWINDOWSSTDIO']='enable'

from dagster import execute_pipeline , pipeline , solid

@solid

def hello_cereal(context):
    dataset_path=os.path.join(os.path.dirname(__file__),'cereal.csv')
    with open(dataset_path,'r') as fd:
        cereals=[row for row in csv.DictReader(fd)]
        context.log.info(
            'Found  {no_cereals} cereals'.format(no_cereals=len(cereals))
        )

    return cereals

@pipeline
def run_pipeline():
    hello_cereal()
