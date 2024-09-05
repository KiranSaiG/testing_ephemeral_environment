import json
import boto3
import os
import uuid

sqs = boto3.client('sqs')
dynamodb = boto3.client('dynamodb')

def lambda_handler(event, context):
    print("Lambda function started.")

    try:
        sqs_queue_url = os.environ.get('SQS_QUEUE_URL', 'DEFAULT_SQS_QUEUE_URL')
        dynamodb_table = os.environ.get('DYNAMODB_TABLE', 'DEFAULT_DYNAMODB_TABLE_NAME')

        if event['httpMethod'] == 'POST':
            # Handle POST request (apitosqs logic)
            return handle_post(event, sqs_queue_url, dynamodb_table)
        elif event['httpMethod'] == 'GET':
            # Handle GET request (apitosqs logic)
            return handle_get(event, dynamodb_table)
        elif event['httpMethod'] == 'DELETE':
            # Handle DELETE request (apitosqs logic)
            return handle_delete(event, sqs_queue_url,dynamodb_table)
        elif event['httpMethod'] == 'PUT':
            return handle_put(event, sqs_queue_url,dynamodb_table)
        
        else:
            # Unsupported HTTP method
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Unsupported HTTP method'})
            }

    except Exception as e:
        print(f"Lambda function error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def handle_post(event, sqs_queue_url, dynamodb_table):
    try:
        print("Received event:", event)
        if isinstance(event['body'], dict):
            body = event['body']
        else:
            # Parse the JSON body if it's a string
            body = json.loads(event['body'])

        if isinstance(body, dict):
            message = body.get('message')
        else:
        # Parse the JSON string to a dictionary
            try:
                parsed_body = json.loads(body)
                message = parsed_body.get('message')
            except json.JSONDecodeError:
                raise ValueError("Error decoding JSON from the event body.")

        print(message)
        message_group_id = body.get('messageGroupId', '123')  # Generate a unique deduplication ID using UUID
        deduplication_id = str(uuid.uuid4())
        if not isinstance(message.get('orderid', 0), int):
            raise ValueError("Invalid data type for 'orderid'. Expected Number.")
        if not isinstance(message.get('product', ''), str):
            raise ValueError("Invalid data type for 'product'. Expected string.")
        if not isinstance(message.get('quantity', 0), (int, float)):
            raise ValueError("Invalid data type for 'quantity'. Expected number.")
        if not isinstance(message.get('customerName', ''), str):
            raise ValueError("Invalid data type for 'customerName'. Expected string.")
        if not isinstance(message.get('shippingAddress', ''), str):
            raise ValueError("Invalid data type for 'shippingAddress'. Expected string.")
            
        orderid = str(message.get('orderid'))    
        existing_record = dynamodb.get_item(
            TableName=dynamodb_table,
            Key={
                'orderid': {'N': orderid}
            }
        )

        if 'Item'  in existing_record:
            raise ValueError(f"Record with orderid '{orderid}' found in the database.")


        # Include the HTTP method in the SQS message
        body['httpMethod'] = event['httpMethod']

        # Send the message to SQS with MessageGroupId and MessageDeduplicationId
        response = sqs.send_message(
            QueueUrl=sqs_queue_url,
            MessageBody=json.dumps(body),
            MessageGroupId=message_group_id,
            MessageDeduplicationId=deduplication_id
        )

        print("Lambda function completed successfully.")
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Message  sent to SQS and stored in DynamoDB successfully'})
        }

    except ValueError as value_error:
        # Handle custom value error
        print(f"Value error: {str(value_error)}")
        return {
            'statusCode': 400,  # Bad Request
            'body': json.dumps({'error': f'Value error: {str(value_error)}'})
        }

def handle_get(event, dynamodb_table):
    try:
        # Extract orderid from the query parameters
        orderid = event['queryStringParameters']['orderid']
    
        # Perform the GET operation in DynamoDB
        read_response = dynamodb.get_item(
            TableName=dynamodb_table,
            Key={
                'orderid': {'N': orderid}
            }
        )

        item_from_dynamodb = read_response.get('Item', {})

        response_data = {
            'statusCode': 200,
            'body': json.dumps({
                'storedData': item_from_dynamodb,
                'message': 'GET request processed successfully'
            })
        }

        print("Response:", response_data)

        # Return the response directly to API Gateway
        return response_data
    except Exception as e:
        print(f"Error: {str(e)}")

        # Return an error response
        error_response = {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

        print("Error Response:", error_response)
        return error_response

def handle_delete(event, sqs_queue_url,dynamodb_table):
    try:
        # Extract orderid from the query parameters
        orderid = event['queryStringParameters']['orderid']
        
        existing_record = dynamodb.get_item(
            TableName=dynamodb_table,
            Key={
                'orderid': {'N': orderid}
            }
        )

        if 'Item' not in existing_record:
            raise ValueError(f"Record with orderid '{orderid}' not found in the database.")

        
        # Send a message to SQS with orderid for DELETE operation and the HTTP method
        sqs.send_message(
            QueueUrl=sqs_queue_url,
            MessageBody=json.dumps({'orderid': orderid, 'httpMethod': 'DELETE'}),
            MessageGroupId='123',  # Modify as needed
            MessageDeduplicationId=str(uuid.uuid4())  # Ensure unique deduplication ID
        )

        # Respond to the client immediately
        response_data = {
            'statusCode': 200,
            'body': json.dumps({'message': 'DELETE request received. Processing in progress.'})
        }

        return response_data

    except Exception as e:
        print(f"Error: {str(e)}")

        # Return an error response
        error_response = {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

        print("Error Response:", error_response)
        return error_response
def handle_put(event, sqs_queue_url,dynamodb_table):
    try:
        print("Received event:", event)
        if isinstance(event['body'], dict):
            body = event['body']
        else:
            # Parse the JSON body if it's a string
            body = json.loads(event['body'])

        if isinstance(body, dict):
            message = body.get('message')
        else:
        # Parse the JSON string to a dictionary
            try:
                parsed_body = json.loads(body)
                message = parsed_body.get('message')
            except json.JSONDecodeError:
                raise ValueError("Error decoding JSON from the event body.")

        print(message)
        if not isinstance(message.get('orderid',0), int):
            raise ValueError("Invalid data type for 'orderid'. Expected Number.")
        if not isinstance(message.get('product', ''), str):
            raise ValueError("Invalid data type for 'product'. Expected string.")
        if not isinstance(message.get('quantity', 0), (int, float)):
            raise ValueError("Invalid data type for 'quantity'. Expected number.")
        if not isinstance(message.get('customerName', ''), str):
            raise ValueError("Invalid data type for 'customerName'. Expected string.")
        if not isinstance(message.get('shippingAddress', ''), str):
            raise ValueError("Invalid data type for 'shippingAddress'. Expected string.")

        
        orderid = str(message.get('orderid'))
       # print(orderid)
       
        # Check if 'orderid' is present in the data
        
        # Check if the record with the given 'orderid' exists in the DynamoDB table
        existing_record = dynamodb.get_item(
            TableName=dynamodb_table,
            Key={
                'orderid': {'N': orderid}
            }
        )

        if 'Item' not in existing_record:
            raise ValueError(f"Record with orderid '{orderid}' not found in the database.")

        # Your validation logic here

        body['httpMethod'] = event['httpMethod']

        response = sqs.send_message(
            QueueUrl=sqs_queue_url,
            MessageBody=json.dumps(body),
            MessageGroupId='123',
            MessageDeduplicationId=str(uuid.uuid4())
        )

        print("Lambda function completed successfully.")
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Data Updated in Table'})
        }

    except ValueError as value_error:
        print(f"Value error: {str(value_error)}")
        return {
            'statusCode': 400,
            'body': json.dumps({'error': f'Value error: {str(value_error)}'})
        }
