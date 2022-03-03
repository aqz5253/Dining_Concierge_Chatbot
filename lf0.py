import json
import boto3
import logging
import datetime
client = boto3.client('lex-runtime')

def lambda_handler(event, context):
    # TODO implement
    
    message = event['messages'][0]['unstructured']['text']
    print("This is Message "+ message)
    

    response = client.post_text(
        botName='OrderFood',
        botAlias='diningBot',
        userId='aqz1998',
        inputText=message)
    
    botMessage={
        "statusCode": "200",
        "messages": [
            {
                "type": "unstructured",
                "unstructured": {
                    "id": "123",
                    "text": response['message']
                }
            }
            ]
    }
    return botMessage