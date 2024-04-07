import boto3
from botocore.exceptions import ClientError
import json
from audit_functions import *
import os
from constants import *


class AWSFunctions:
    def __init__(self) -> None:
       pass

    def readSecrets(self,region_name,secret_name) -> str :
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            raise e
        secret_data = get_secret_value_response['SecretString']
        password = json.loads(secret_data)
        return password['password']
    
    def upload_file_to_s3(self,file_name, bucket_name, object_name=None):
        # If S3 object_name is not specified, use file_name
        if object_name is None or object_name=='':
            object_name = file_name.split('/')[-1]

        # Upload the file
        s3_client = boto3.client('s3')
        response = s3_client.upload_file(file_name, bucket_name, object_name)
        s3_path = f's3://{bucket_name}/{object_name}'
        #logging(logtype=Log.info,msg="Upload successful")
        return s3_path
    
    def download_files_from_s3(self,bucket_name, string_match, local_directory):
        # Initialize the S3 client
        s3_client = boto3.client('s3')

        # List all objects in the bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name,Prefix=string_match)
        for obj in response.get('Contents', []):
            # Check if the object key contains the specified string
            if string_match in obj['Key'] and not obj['Key'].endswith("/"):
                # Download the object to the local directory
                local_file_path = os.path.join(local_directory,'')
                filename = obj['Key'].split('/')[-1]
                s3_client.download_file(bucket_name, obj['Key'], local_file_path+filename)

                #logging(logtype=Log.info,msg=f"Downloaded: {obj['Key']} to {local_file_path}")
    def delete_file_in_s3(self,bucket_name, key):
        try:
            s3 = boto3.client('s3')
            # Delete the file
            s3.delete_object(Bucket=bucket_name, Key=key)
            print(f"File '{key}' deleted successfully from S3.")
        except Exception as e:
            print(f"Error deleting file from S3: {e}")
    def archive_files(self,archive,bucket_name,source_file,source_path):
        try:
            # Copy the file
            s3 = boto3.client('s3')
            s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': source_path+source_file}, Key=archive+source_file)
            self.delete_file_in_s3(bucket_name=bucket_name,key=source_path+source_file)
        except Exception as e:
            print(f"Error copying file in S3: {e}")

    def send_message(self,message_body,queue_url,region_name,message_group_id,message_deduplication_id):
        sqs = boto3.client('sqs', region_name=region_name)
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body,
            MessageGroupId=message_group_id,
            MessageDeduplicationId=message_deduplication_id
        )
        print("Message sent:", response)
    
    def receive_messages(self,queue_url,region_name):
        sqs = boto3.client('sqs', region_name=region_name)
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20 
        )
        if 'Messages' in response:
            for message in response['Messages']:
                print("Received message:", message['Body'])
    
    def mappedSQS(self,avropath,avrodata):
        messageGroupId = commonFunctions.generate_random_string(12)
        arvoFile =  avrodata
        Body = 'Hi'
        messageDeduplicationID = commonFunctions.hash_string(avropath.split('/')[-1])
        fileName = avropath.split('/')[-1]
        self.send_message(queue_url='https://sqs.us-east-1.amazonaws.com/243496286433/avro.fifo',region_name='us-east-1',
                          message_body=str(arvoFile),message_group_id=messageGroupId,message_deduplication_id=messageDeduplicationID)
        
    def sendMail(tself,toMail,fromMail,sub,body,AWS_REGION):
        CHARSET = "UTF-8"
        client = boto3.client('ses',region_name=AWS_REGION)
        try:
            #Provide the contents of the email.
            response = client.send_email(
                Destination={
                    'ToAddresses': [
                        toMail,
                    ],
                },
                Message={
                    'Body': {
                        'Text': {
                            'Charset': CHARSET,
                            'Data': body,
                        },
                    },
                    'Subject': {
                        'Charset': CHARSET,
                        'Data': sub,
                    },
                },
                Source=fromMail
            )
        # Display an error if something goes wrong.	
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            print("Email sent! Message ID:"),
            print(response['MessageId'])

    
   
