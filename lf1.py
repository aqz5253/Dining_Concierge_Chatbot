import math
from urllib import response
import dateutil.parser
import datetime
import time
import os
import logging
import re
import boto3
import json

logger = logging.getLogger()
sqs_client=boto3.client('sqs')
queue_url=sqs_client.get_queue_url(QueueName='dining_request_queue').get('QueueUrl')


def get_slots(intent_request):
    return intent_request['currentIntent']['slots']


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')


def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot,
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False


def validate_dining_request(location,cuisine,date,reservation_time,no_of_people,phone_number,email):
    cuisine_types = ['chinese', 'japanese', 'italian','mexican','tradamerican']

    if location is not None and location.lower() != 'new york':
        return build_validation_result(False,'Location','I can only help you with restaurants in New York')

    if cuisine is not None and cuisine.lower() not in cuisine_types:
        return build_validation_result(False,
                                       'Cuisine',
                                       'We do not have {}, Please pick a different Cuisine. '
                                       'We have good recommendations for Italian, Chinese, Japanese, Mexican and American'.format(cuisine))

    if date is not None:
        if not isvalid_date(date):
            return build_validation_result(False, 'Date', 'I did not understand that, what date would you like to make the reservation?')
        elif datetime.datetime.strptime(date, '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(False, 'Date', 'You can book a reservation only from today onwards.  What day would you like to reserve?')
    
    if reservation_time is not None:
        new_time= date +" "+ reservation_time
        if datetime.datetime.strptime(new_time,'%Y-%m-%d %H:%M') < datetime.datetime.now():
            return build_validation_result(False,'Time',"Please enter a valid time")
        
    if no_of_people is not None:
        no_of_people=parse_int(no_of_people)
        if no_of_people <=0 or no_of_people > 20:
            return build_validation_result(False,'TotalPeople','Please enter a a number greater than 0 and less than 20')
        
    if phone_number is not None:
        regex= "\w{3}\w{3}\w{4}"
        if not re.search(regex, phone_number):
            logger.debug("The phone number {} is not valid".format(phone_number))
            return build_validation_result(False,
                                            'PhoneNo',
                                            'Please enter a valid phone number.')     
    if email is not None:
        regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        if not re.fullmatch(regex, email):
            logger.debug("The email {} is not valid".format(email))
            return build_validation_result(False,
                                            'Email',
                                            'Please enter a valid email address.')

    print("Returning true")
    return build_validation_result(True, None, None)




def dining_suggestion_intent(intent_request):

    location=get_slots(intent_request)["Location"]
    cuisine = get_slots(intent_request)["Cuisine"]
    
    if cuisine and cuisine.lower()=='american':
        cuisine='tradamerican'
        
    date = get_slots(intent_request)["Date"]
    time = get_slots(intent_request)["Time"]
    no_of_people= get_slots(intent_request)["TotalPeople"]
    phone_number=get_slots(intent_request)["PhoneNo"]
    email= get_slots(intent_request)["Email"]
    source = intent_request['invocationSource']

    if source == 'DialogCodeHook':
        
        slots = get_slots(intent_request)

        validation_result = validate_dining_request(location,cuisine, date, time, no_of_people,phone_number,email)
        print( validation_result)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])

        
        output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
        
        
        push_to_sqs = True
        queue_msg={"city":location,"cuisine":cuisine,"date":date,"time":time,"no_of_people":no_of_people,"phone_number":phone_number,"email":email}
        for key,val in queue_msg.items():
            if val == None:
                push_to_sqs = False
                break
        
        if push_to_sqs:
            print("Pushing to the queue")
            print(queue_msg)
            response=sqs_client.send_message(QueueUrl=queue_url,MessageBody=json.dumps(queue_msg))
        
        return delegate(output_session_attributes, get_slots(intent_request))
        
        

    
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': "You are good to go! I will send my suggestions in a while."})


def greeting_intent(intent_request):
    output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    return close(output_session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': "Hi there!!! How can I help you ?"
        })

def thankyou_intent(intent_request):
    output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    return close(output_session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': "You are welcome!!!"
        })

def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))
    
    intent_name = intent_request['currentIntent']['name']

    # Dispatch to your bot's intent handlers
    if intent_name == 'GreetingIntent':
        return greeting_intent(intent_request)
    elif intent_name == 'DiningSuggestionsIntent':
        return dining_suggestion_intent(intent_request)
    elif intent_name=='ThankyouIntent':
        return thankyou_intent(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')



def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event)
