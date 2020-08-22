from dagster import execute_pipeline , pipeline,solid

@solid
def get_number(_):
 yield [1,3]


@solid
def get_sum(context,a:list):
    context.log.info("Result is : "+str(sum(a)))
    print("Output from print method :{0} ".format( sum(a)))
@pipeline
def run_pipeline():
    get_sum(get_number())

if __name__=="__main__":
    execute_pipeline(run_pipeline)