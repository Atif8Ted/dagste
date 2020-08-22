from dagster import pipeline , execute_pipeline,solid,Output,OutputDefinition
@solid(output_defs=[OutputDefinition(name="output_1"), OutputDefinition(name="output_2")])

def multiple_output_solid(context):
    yield Output(1, output_name="output_1")
    yield Output(2, output_name="output_2")


@solid
def print_output(context, output_tup):
    print("Outputs are {0}  {1}".format(output_tup.output_1,output_tup.output_2))

@pipeline
def run_pipeline():
    print_output(multiple_output_solid())

if __name__=="__main__":
    execute_pipeline(run_pipeline)


