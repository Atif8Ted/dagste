from dagster import execute_pipeline , pipeline,solid,Output

@solid
def my_yield_solid(context):
    yield Output(9999999999999)



@solid
def print_solid(context,out_put):
    context.log.info("Printed output "+str(out_put))
    print("Printed Module output "+str(out_put))

@pipeline
def run_pipeline():
    print_solid(my_yield_solid())


if __name__=="__main__":
    execute_pipeline(run_pipeline)


