import json
import boto3
client = boto3.client('lex-runtime')

def lambda_handler(event, context):
    # TODO implement
    
    print(event)
    
    client = boto3.client('lex-runtime')
    
    response = client.post_text(
        botName='DiningChatBot',
        botAlias='DiningChatBot',
    
        userId='string',
        
        sessionAttributes={
            'string': 'string'
        },
        requestAttributes={
            'string': 'string'
        },
        
        inputText = event['lastUserMessage']
    )
    
    return (response['message'])