#!/usr/bin/env python
"""
Client which receives the requests

Args:
    API Token
    API Base (https://...)

    TOM TEST

"""
from flask import Flask, request
import logging
import argparse
import urllib2
import boto3
import json
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr

logging.basicConfig(filename='logging.log',level=logging.DEBUG)

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
handled_ids_table = dynamodb.Table('handled_ids')
transient_messages_table = dynamodb.Table('transient_messages')

# parsing arguments
PARSER = argparse.ArgumentParser(description='Client message processor')
PARSER.add_argument('API_token', help="the individual API token given to your team")
PARSER.add_argument('API_base', help="the base URL for the game API")

ARGS = PARSER.parse_args()

# defining global vars
MESSAGES = {} # A dictionary that contains message parts
API_BASE = ARGS.API_base
# 'https://csm45mnow5.execute-api.us-west-2.amazonaws.com/dev'

APP = Flask(__name__)

# creating flask route for type argument
@APP.route('/', methods=['GET', 'POST'])
def main_handler():
    """
    main routing for requests
    """
    if request.method == 'POST':
        # APP.logger.debug("Received POST: %s" % json.dumps(request.get_json()))
        result = process_message(request.get_json())
        process_message_dynamo(request.get_json())
        return result
    else:
        return get_message_stats()

def get_message_stats():
    """
    provides a status that players can check
    """
    msg_count = len(MESSAGES.keys())
    return "There are %d messages in the MESSAGES dictionary" % msg_count

def process_message(msg):
    """
    processes the messages by combining and appending the kind code
    """
    APP.logger.debug("Processing a msg_id %s" % msg['Id'])

    msg_id = msg['Id'] # The unique ID for this message
    total_parts = msg['TotalParts']
    part_number = msg['PartNumber'] # Which part of the message it is
    data = msg['Data'] # The data of the message

    # Try to get the parts of the message from the MESSAGES dictionary.
    # If it's not there, create one that has None in both parts
    parts = MESSAGES.get(msg_id, [None for i in range(total_parts)])

    # store this part of the message in the correct part of the list
    parts[part_number] = data

    # store the parts in MESSAGES
    MESSAGES[msg_id] = parts

    # if both parts are filled, the message is complete
    if None not in parts:
        APP.logger.debug("got a complete message for %s" % msg_id)

        response = handled_ids_table.query(KeyConditionExpression=Key('id').eq(msg_id))
        
        items = response.get('Items')
        if items:
            APP.logger.debug("Skipped. All ready responded for item: %s" % msg_id)
            return 'OK'

        # We can build the final message.
        result = ''.join(parts)
        # sending the response to the score calculator
        # format:
        #   url -> api_base/jFgwN4GvTB1D2QiQsQ8GHwQUbbIJBS6r7ko9RVthXCJqAiobMsLRmsuwZRQTlOEW
        #   headers -> x-gameday-token = API_token
        #   data -> EaXA2G8cVTj1LGuRgv8ZhaGMLpJN2IKBwC5eYzAPNlJwkN4Qu1DIaI3H1zyUdf1H5NITR
        APP.logger.debug("ID: %s" % msg_id)
        APP.logger.debug("RESULT: %s" % result)
        url = API_BASE + '/' + msg_id
        APP.logger.debug("Sending response to %s" % url)
        req = urllib2.Request(url, data=result, headers={'x-gameday-token':ARGS.API_token})
        resp = urllib2.urlopen(req)
        server_response = resp.read()
        resp.close()

        handled_ids_table.put_item(
            Item={
                    'id': msg_id,
                    'sentdate': datetime.now().strftime("%Y-%m-%d-%H:%M:%S"),
                    'data': result
                }
            )

        APP.logger.debug(server_response)

    return 'OK'

def process_message_dynamo(msg):
    """
    processes the messages by combining and appending the kind code
    """
    APP.logger.debug("DYNAMO: Processing a msg_id %s" % msg['Id'])

    msg_id = msg['Id'] # The unique ID for this message
    total_parts = msg['TotalParts']
    part_number = msg['PartNumber'] # Which part of the message it is
    data = msg['Data'] # The data of the message

    # Try to fetch the existing item
    response = transient_messages_table.query(KeyConditionExpression=Key('id').eq(msg_id))
    item = response.get('Items')

    if not item:
        APP.logger.debug("DYNAMO: New item")
        parts = ['PENDING' for i in range(total_parts)]
    else:
        APP.logger.debug("DYNAMO: Existing item")
        parts = item.get('parts_data')
        APP.logger.debug("DYNAMO: existing parts: %s" % parts)
    
    parts[part_number] = data
    APP.logger.debug("DYNAMO: %s" % ','.join(parts))

    transient_messages_table.put_item(
        Item={
            'id': msg_id,
            'modified_date': datetime.now().strftime("%Y-%m-%d-%H:%M:%S"),
            'parts_data': parts
        }
    )

    if 'PENDING' not in parts:
        APP.logger.debug("DYNAMO: Ready to send msg_id %s" % msg['Id'])

    return 'OK'

if __name__ == "__main__":

    # By default, we disable threading for "debugging" purposes.
    # This will cause the app to block requests, which means that you miss out on some points,
    # and fail ALB healthchecks, but whatever I know I'm getting fired on Friday.
    APP.logger.debug("Starting application")
    APP.run(host="0.0.0.0", port=80)
    
    # Use this to enable threading:
    # APP.run(host="0.0.0.0", port=80, threaded=True)
