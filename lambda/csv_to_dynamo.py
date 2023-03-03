import json
import csv
import boto3
from datetime import datetime


def lambda_handler(event, context):
    region = 'us-west-2'
    record_list = []
    
    try:
        s3= boto3.client('s3')
        dynamodb = boto3.client('dynamodb', region_name = region)
        
        #bucket = event["Records"][0]['s3']['bucket']['name']
        #key = event["Records"][0]['s3']['object']['key']
        
        csv_file = s3.get_object(Bucket='medispan-poc', Key=f'output/12-02-2022-M25Export-Records.csv')
        
        record_list = csv_file['Body'].read().decode('utf-8').split('\n')
        
        csv_reader = csv.reader(record_list, delimiter=',')
        
        for row in csv_reader:
            NDC = row[1]
            IsMaintenanceDrug = row[2]
            RouteOfAdmin = row[3]
            GPICode = row[4]
            GPIGenericName = row[5]
            NDCName = row[6]
            GenericIngredientName = row[7]
            WACUnitPrice = row[8]
            WACEffectiveDate = row[9] 
            AWPUnitPrice = row[10]
            IsAWP = row[11]
            AWPEffectiveDate = row[12]
            datetime = row[13]
            id = row[14]
            
            add_to_db = dynamodb.put_item(
                TableName = 'medispan-csv-dynamo2',
                Item = {
                    'NDC': {'S': str(NDC)},
                    'IsMaintenanceDrug': {'S': str(IsMaintenanceDrug)},
                    'RouteOfAdmin': {'S': str(RouteOfAdmin)},
                    'GPICode': {'S': str(GPICode)},
                    'GPIGenericName': {'S': str(GPIGenericName)},
                    'NDCName': {'S': str(NDCName)},
                    'GenericIngredientName': {'S': str(GenericIngredientName)},
                    'WACUnitPrice': {'S': str(WACUnitPrice)},
                    'WACEffectiveDate': {'S': str(WACEffectiveDate)},
                    'AWPUnitPrice': {'S': str(AWPUnitPrice)},
                    'IsAWP': {'S': str(IsAWP)},
                    'AWPEffectiveDate': {'S': str(AWPEffectiveDate)},
                    'datetime': {'S': str(datetime)},
                    'id': {'S': str(id)},
                    
                    
                })
            print("sucessully uploaded")
    
    except Exception as 
