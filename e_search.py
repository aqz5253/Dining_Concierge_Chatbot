from email import header
import json
from urllib import response
import boto3
import requests
from boto3.dynamodb.conditions import Key
#from botocore.vendored import requests

url='https://search-yelp-dining-4vjeskyq2dilmwvso7qkqyookm.us-east-1.es.amazonaws.com/diningrestaurant/restaurant'
headers = {"Content-Type": "application/json"}
dynamodb = boto3.resource('dynamodb', region_name='us-east-1',aws_access_key_id='',aws_secret_access_key='')
table = dynamodb.Table('yelp-restaurants')
print(table)
result= table.scan()

inserted_values=1

while True:
    for item in result['Items']:
        body = {"RestaurantID": item['business_id'], "Cuisine": item['cuisine']}
        print(body)
        r = requests.post(url, data=json.dumps(body).encode("utf-8"), headers=headers, auth=('',''))
        print(r)
        inserted_values += 1
    if 'LastEvaluatedKey' in result:
        result = table.scan(
        ExclusiveStartKey=result['LastEvaluatedKey']
        )
    else:
        break
print(inserted_values)



