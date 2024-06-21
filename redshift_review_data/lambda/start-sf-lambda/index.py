import boto3
import traceback
import json
import os

from aws_lambda_powertools import Logger
#from aws_lambda_powertools import Tracer
#from aws_lambda_powertools import Metrics
logger = Logger()
#metrics = Metrics(namespace="PowertoolsSample")
import cfnresponse

bucket = os.environ["BUCKET_NAME"]

def create_global_state():
    with open('../config/workflow.json') as f:
        global_state = json.load(f)
        for step in global_state:
            global_state[step]["Status"] = "INITIAL"
            global_state[step]["Script"] = step
            global_state[step]["ScriptName"] = os.environ["SCRIPT_PATH"] + "rpr_ " + step + ".sql"
            global_state[step]["OutputLocation"] = os.environ["RESULT_PATH"] +++ step + ".json"
            global_state[step]["ErrorLocation"] = os.environ["ERROR_PATH"]

        f.close()
    workflow = {}
    workflow["QueryList"] = global_state
    workflow["S3BucketName"] = bucket
    workflow["Query"] =  global_state[global_state.keys()[0]]
    return global_state



def handler(event, context):
    logger.info(event)
    step_function_client = boto3.client('stepfunctions')
    res = {}
    if event['RequestType'] != 'Delete':
        
        try:
            step_function_input = {"comment": "Execute ETL Workflow for Redshift"}
            response = step_function_client.start_execution(stateMachineArn=event['ResourceProperties'].get('StepFunctionArn'),
                                                            input=json.dumps(global_state)
                                                            )
            print(response)
        except:
            print(traceback.format_exc())
            cfnresponse.send(event, context, cfnresponse.FAILED, input)
            raise
    cfnresponse.send(event, context, cfnresponse.SUCCESS, res)