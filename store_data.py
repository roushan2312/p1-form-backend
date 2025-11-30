import boto3
from datetime import datetime
import os

table_name = os.environ.get('DYNAMODB_TABLE')
gsi_name = os.environ.get('DYNAMODB_GSI_EMAIL')
counter_table_name = os.environ.get('DYNAMODB_COUNTER_TABLE')

def store_data_function(event):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    counter_table = dynamodb.Table(counter_table_name)

    def search_email(email):
        response_1 = table.query(
            IndexName=gsi_name,
            KeyConditionExpression=boto3.dynamodb.conditions.Key('Email').eq(email),
            Select='COUNT'
        )

        return response_1['Count'] == 0 # True if email not found
    
    def get_next_id():
        response = counter_table.update_item(
            Key={'CounterName': table_name},
            UpdateExpression='SET CounterValue = CounterValue + :inc',
            ExpressionAttributeValues={':inc': 1},
            ReturnValues='UPDATED_NEW'
        )
        return int(response['Attributes']['CounterValue'])

    if search_email(event['email']):
        id = get_next_id()
        Id = str(id).zfill(6)  # Zero-padded to 6 digits
        final_id = "FORM-" + Id
        item = {
            'ID': final_id,
            'Name': event.get('name'),
            'Email': event.get('email'),
            'Phone': event.get('phone'),
            'Bank_Account_Name': event.get('bank_account_name'),
            'Bank_Name': event.get('bank_name'),
            'Account_Number': event.get('account_number'),
            'IFSC_Code': event.get('ifsc_code'),
            'Branch': event.get('branch'),
            'PAN_Number': event.get('pan_number'),
            'Business_Name': event.get('business_name'),
            'Business_Type': event.get('business_type'),
            'Form_Timestamp': event.get('timestamp'),
            'DynamoDB_Timestamp': datetime.utcnow().isoformat(),
            'Status': 'FORM RECEIVED'
        }

        try:
            table.put_item(Item=item)
            return {'id': id, 'status': 'success'}
        except Exception as e:
            return {'id': id, 'status': 'error', 'error_message': str(e)}
    else:
        return {'status': 'duplicate_email'}
