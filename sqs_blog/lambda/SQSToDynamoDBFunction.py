import json
import os
import boto3
import uuid
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info(f'Received {len(event["Records"])} messages from SQS')
    
    try:
        messages = event['Records']
        dynamodb_client = boto3.client('dynamodb')
        table_name = os.environ['DYNAMODB_TABLE_NAME']
        
        successful_records = 0
        
        for message in messages:
            try:
                message_body = json.loads(message['body'])
                record_id = str(uuid.uuid4())
                
                logger.debug(f'Processing message ID: {message["messageId"]}')
                
                item = {
                    'id': {'S': record_id},
                    'product_id': {'S': message_body['product_id']},
                    'location': {'S': message_body['location']},
                    'quantity': {'N': str(message_body['quantity'])},
                    'update_date': {'S': message_body['update_date']}
                }
                
                dynamodb_client.put_item(TableName=table_name, Item=item)
                successful_records += 1
                
            except Exception as e:
                logger.error(f'Error processing message {message.get("messageId", "unknown")}: {str(e)}')
                logger.error(f'Message body: {message.get("body", "No body")}')
                # Continue processing other messages even if one fails
                continue
        
        logger.info(f'Successfully processed {successful_records} out of {len(messages)} records to DynamoDB')
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully processed {successful_records} records')
        }
        
    except Exception as e:
        logger.error(f'Fatal error in Lambda handler: {str(e)}')
        raise
