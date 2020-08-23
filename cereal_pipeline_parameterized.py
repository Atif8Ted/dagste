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
    sorted_

