from dagster import DependencyDefinition, InputDefinition, PipelineDefinition, pipeline, solid,execute_pipeline


@solid
def return_one(context):
    return 1


@solid(input_defs=[InputDefinition("number", int)])
def add_one(context, number):
    return number + 1


@pipeline
def one_plus_one_pipeline():
    add_one(return_one())


if __name__=="__main__":
    execute_pipeline(add_one)