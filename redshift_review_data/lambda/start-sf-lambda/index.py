import boto3
import traceback
import json

from aws_lambda_powertools import Logger
#from aws_lambda_powertools import Tracer
#from aws_lambda_powertools import Metrics

logger = Logger()
#metrics = Metrics(namespace="PowertoolsSample")
import cfnresponse
def handler(event, context):
    logger.info(event)
    step_function_client = boto3.client('stepfunctions')
    res = {}
    if event['RequestType'] != 'Delete':
        try:
            step_function_input = {"comment": "Execute ETL Workflow for Redshift"}
            response = step_function_client.start_execution(stateMachineArn=event['ResourceProperties'].get('StepFunctionArn'),
                                                            input=json.dumps(step_function_input)
                                                            )
            print(response)
        except:
            print(traceback.format_exc())
            cfnresponse.send(event, context, cfnresponse.FAILED, input)
            raise
    cfnresponse.send(event, context, cfnresponse.SUCCESS, res)