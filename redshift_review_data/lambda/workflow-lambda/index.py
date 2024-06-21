import boto3
import traceback
import json
from uuid import uuid4


def lambda_handler(event, context):
    # TODO implement
    print(event)
    state = json.loads(event.get("input").get("Input"))
    bucket = state.get("S3BucketName")
    query = state.get("Query")
    workflow = state.get("Workflow")
    script = query.get("Script")
    status = event.get("input").get("Status")
    
    uuid_str = str(uuid4())
    last_string = uuid_str.split('-')[-1]
    
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
            "Query": {
                "OutputLocation": f"manifest/workflow-{last_string}.manifest"
            },
            "S3BucketName": bucket
        },
        'continue': False
    }