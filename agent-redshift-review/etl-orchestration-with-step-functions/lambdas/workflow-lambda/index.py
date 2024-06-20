import boto3
import traceback
import json

from aws_lambda_powertools import Logger
#from aws_lambda_powertools import Tracer
#from aws_lambda_powertools import Metrics

logger = Logger()
#metrics = Metrics(namespace="PowertoolsSample")
import json

import boto3
import traceback
import json


def lambda_handler(event, context):
    # TODO implement
    print(event)
    state = json.loads(event.get("input").get("Input"))
    bucket = state.get("S3BucketName")
    query = state.get("Query")
    workflow = state.get("Workflow")
    script = query.get("Script")
    status = event.get("input").get("Status")
    
    print(script)
    print(status)
    
    next = 0
    
    for step in workflow:
        print(step)
        if next == 1:
            next_query = workflow[step]
            return { 
                    'statusCode': 200,
                    'body': {
                        "QueryList": workflow,
                        "Query": next_query,
                        "S3BucketName": bucket
                    },
                    'continue': True
                }
        if step == script:
            workflow[step]["Status"] = status
            next = 1
            
    return { 
        'statusCode': 200,
        'body': {
            "QueryList": workflow,
            "Query": "",
            "S3BucketName": bucket
        },
        'continue': False
    }