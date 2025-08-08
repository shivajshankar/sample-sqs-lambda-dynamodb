import json
import boto3
import csv
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info(f'Received event: {json.dumps(event)}')
    
    try:
        record = event['Records'][0]
        source_bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        logger.info(f'Processing file s3://{source_bucket}/{key}')

        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=source_bucket, Key=key)
        csv_content = response['Body'].read().decode('utf-8-sig')

        sqs_client = boto3.client('sqs')
        queue_url = os.environ['SQS_QUEUE_URL']
        
        csv_reader = csv.DictReader(csv_content.splitlines())
        message_batch = []
        total_messages = 0
        
        for row in csv_reader:
            json_message = json.dumps(row)
            message_batch.append({
                'Id': str(len(message_batch) + 1),
                'MessageBody': json_message
            })

            if len(message_batch) == 10:
                sqs_client.send_message_batch(
                    QueueUrl=queue_url,
                    Entries=message_batch
                )
                total_messages += len(message_batch)
                logger.info(f'Sent batch of {len(message_batch)} messages to SQS')
                message_batch = []

        if message_batch:
            sqs_client.send_message_batch(
                QueueUrl=queue_url,
                Entries=message_batch
            )
            total_messages += len(message_batch)
            logger.info(f'Sent final batch of {len(message_batch)} messages to SQS')
        
        logger.info(f'Successfully processed {total_messages} messages to SQS')
        
    except Exception as e:
        logger.error(f'Error processing CSV: {str(e)}')
        raise
