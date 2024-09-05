import json
import boto3
import os
import uuid

dynamodb = boto3.client('dynamodb')
sqs = boto3.client('sqs')

# Fetch environment variables
table_name = os.environ['DYNAMODB_TABLE']
queue_url = os.environ['SQS_QUEUE_URL']

def lambda_handler(event, context):
    print("Lambda function started.")

    try:
        # Assuming the SQS message body is a plain string
        #message_body = event['Records'][0]['body']

        # Parse the JSON message body
        #order_data = json.loads(message_body)
        order_data=event['Records'][0]
        #order_message = json.loads(order_data.get('body', '{}')).get('message', {})
        order_body=json.loads(order_data.get('body', '{}'))
        order_message = order_body.get('message', {})
        http_method=order_body.get('httpMethod','')
        # Check if "httpMethod" is present in the SQS message
        #if 'httpMethod' in order_data:
         #   http_method = order_data['httpMethod']
        #else:
            # If "httpMethod" is not present, default to 'POST'
            #http_method = 'POST'
         #   print("HttpMethod not found")

        if http_method == 'POST':
            # Add logic to process and store data in DynamoDB for POST operation
            try:
                # Extract order data from the SQS message
                response = dynamodb.put_item(
                    TableName=table_name,
                    Item={
                        'orderid': {'N': str(order_message.get('orderid', str(uuid.uuid4())))},
                        'product': {'S': order_message.get('product')},
                        'quantity': {'N': str(order_message.get('quantity', 0))},
                        'customerName': {'S': order_message.get('customerName', '')},
                        'shippingAddress': {'S': order_message.get('shippingAddress', '')}
                    }
                )

                print("Order processed and stored successfully.")
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'Order processed and stored successfully',
                        'dynamoDBResponse': response  # Include DynamoDB response in the API response
                    })
                    
                }

            except Exception as e:
                print(f"Error: {str(e)}")
                # Return an error response
                return {
                    'statusCode': 500,
                    'body': json.dumps({'error': str(e)})
                }

        elif http_method == 'DELETE':
            # Add logic to handle DELETE operation
            try:
                # Extract orderid from the SQS message
                orderid = order_body.get('orderid')

                # Perform the delete operation in DynamoDB
                response = dynamodb.delete_item(
                    TableName=table_name,
                    Key={
                        'orderid': {'N': orderid}
                    }
                )

                print("Order deleted successfully.")
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'Order deleted successfully',
                        'dynamoDBResponse': response  # Include DynamoDB response in the API response
                    })
                }

            except Exception as e:
                print(f"Error: {str(e)}")
                # Return an error response
                return {
                    'statusCode': 500,
                    'body': json.dumps({'error': str(e)})
                }
        elif http_method == 'PUT':
            return handle_put(order_message)
        
        else:
            raise ValueError(f"Unsupported operation: {http_method}")

    except Exception as e:
        print(f"Error: {str(e)}")
        # Return an error response
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
def handle_put(order_message):
    try:
        # Check if 'orderid' is present in the order_message
        
        # If the record exists, update it
        response = dynamodb.put_item(
            TableName=table_name,
            Item={
                'orderid': {'N': str(order_message.get('orderid'))},
                'product': {'S': order_message.get('product', '')},
                'quantity': {'N': str(order_message.get('quantity', 0))},
                'customerName': {'S': order_message.get('customerName', '')},
                'shippingAddress': {'S': order_message.get('shippingAddress', '')}
            }
        )

        print("Order updated and stored successfully.")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Order updated and stored successfully',
                'dynamoDBResponse': response
            })
        }

    except ValueError as ve:
        print(f"Error: {str(ve)}")
        return {
            'statusCode': 400,  # Bad Request
            'body': json.dumps({'error': str(ve)})
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
