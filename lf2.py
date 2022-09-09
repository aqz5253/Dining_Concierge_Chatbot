from wsgiref import headers
import boto3
import json
import logging
import requests
from boto3.dynamodb.conditions import Key, Attr
from requests_aws4auth import AWS4Auth
logger = logging.getLogger()


def lambda_handler(event, context):
    # TODO implement
    sqs_client=boto3.client('sqs')
    dynamodb=boto3.resource('dynamodb')
    ses_client = boto3.client('ses')

    
    queue_url=sqs_client.get_queue_url(QueueName='dining_request_queue')['QueueUrl']
    response=sqs_client.receive_message(QueueUrl=queue_url,AttributeNames=['All'])
    
    table = dynamodb.Table('yelp-restaurants')
    

    try:
        message = response['Messages']
        
    except KeyError:
        print('No messages in the queue!')
        return

    message_body=json.loads(message[0]['Body'])
    
    cuisine=message_body['cuisine']
    date=message_body['date']
    time=message_body['time']
    no_of_people=message_body['no_of_people']
    phone_number=message_body['phone_number']
    email=message_body['email']

    print(cuisine +" "+date+" "+time+" "+no_of_people+" "+phone_number+" "+email)

    url='https://search-yelp-dining-4vjeskyq2dilmwvso7qkqyookm.us-east-1.es.amazonaws.com/diningrestaurant/_search'
    cred=boto3.Session().get_credentials()
    auth = AWS4Auth(cred.access_key, cred.secret_key, 'us-east-1', 'es', session_token=cred.token)


    query = {
        "size": 5,
          "query": {
              "multi_match": {
                  "query": cuisine,
                  "fields": ["Cuisine"]
              }
          }
        }
    headers = { "Content-Type": "application/json" }
    response=requests.get(url,auth=('aqz1998',''),headers=headers,data=json.dumps(query))
    data = json.loads(response.content.decode('utf-8'))
    print(data)
    
    data_es = data["hits"]["hits"]
    
    restaurant_ids=[]
    for document in data_es:
        restaurant_ids.append(document['_source']['RestaurantID'])
        
    print("This are the ids")
    print (restaurant_ids)
    
    suggestion_message='Hi, here are my suggestions for '+cuisine+' restaurants suggestions in New York for '+ no_of_people+' people on '+date+' at '+time;
    
    counter=1
    
    for r_id in restaurant_ids:
        print("searching for")
        print( r_id)
        response=table.scan(FilterExpression=Attr('business_id').eq(r_id))
        
        if response is None:
            print("getting out")
            continue
        
        #print("response")
       # print(response)
        
        item=response['Items'][0]
        name=item['name']
        address=item['address']
        
        temp_message= '\n'+str(counter)+". "+name+' located at '+address+'.'
        print("New rest ")
        print(temp_message)
        suggestion_message= suggestion_message+" "+temp_message
        counter=counter+1
    
    suggestion_message+="\nHave a great day!!"
    
    
    print("Final_messs "+suggestion_message)
    
    destination={
        'ToAddresses':['abdulqadirzakir@gmail.com']
    }
    email_content={
        'Subject': {
            'Data': 'Dining Suggestions'
        },
        'Body': {
            'Text': {
                'Data': suggestion_message
            }
        }
    }
    
    email_reponse=ses_client.send_email(Source='abdulqadirzakir@gmail.com',Destination=destination,Message=email_content)
    
    print("email response below")
    print(email_reponse)
    
    try:
        sqs_client.delete_message(QueueUrl = queue_url, ReceiptHandle = message[0]['ReceiptHandle'])

    except:
        raise RuntimeError("Failed to delete messages!")
