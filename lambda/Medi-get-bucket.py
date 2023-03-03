try:
    import json
    import boto3
    import pandas as pd
    import time
    import io
    print("All Modules are ok ...")

except Exception as e:
    print("Error in Imports ")

s3_client = boto3.client('s3')

# df_a1 = pd.Series(dtype='object', name='test1')
# df_g1 = pd.Series(dtype='object', name='test2')
# df_j1 = pd.Series(dtype='object', name='test3')
# df_p1 = pd.Series(dtype='object', name='test4')
# df_r1 = pd.Series(dtype='object', name='test5')
# df_q1 = pd.Series(dtype='object', name='test6')





def lambda_handler(event, context):
    print(event)
    #Get Bucket Name
    bucket = event['Records'][0]['s3']['bucket']['name']

    #get the file/key name
    key = event['Records'][0]['s3']['object']['key']
    response = s3_client.get_object(Bucket=bucket, Key=key)
    print("Got Bucket! - pass")
    print("Got Name! - pass ")
    
    data = response['Body'].read().decode('utf-8')
    print('reading data')
    buf = io.StringIO(data)
    print(buf.readline())
    #data is the file uploaded
    fileRow = buf.readline()

    
    print('reading_row')
    while fileRow: 
            
        currentString = fileRow
        ndc = currentString[0:11].strip() 
        recordcode = currentString[12:15].strip() #this grabs the code the indicates what the data type is
    
        #controls which function to run based on the code
        switcher = {
        "A1": Add_A1,
        "J1": Add_J1,
        "G1": Add_G1,
        "P01": Add_P01,
        "R01": Add_R01,
        "Q01": Add_Q01        
        }
        runfunc = switcher.get(recordcode, errorHandler)
        runfunc (ndc, recordcode, currentString)
        fileRow = buf.readline()
        print(type(df_a1), "A1 FILE")
        print(type(df_g1), 'G1 FILE')
    buf.close()
    
    ##########STEP 3: JOIN THE DATA TOGETHER##########
    try:
        df_merge = pd.merge(df_a1, df_g1, how="left", on="NDC")
        df_merge = pd.merge(df_merge, df_j1, how="left", on="NDC")
        df_merge = pd.merge(df_merge, df_p1, how="left", on="NDC")
        df_merge = pd.merge(df_merge, df_q1, how="left", on="NDC")
        df_merge = pd.merge(df_merge, df_r1, how="left", on="NDC")
    except Exception as e:
        print(e, "Error in merging ")
    
    ##########STEP 4: SAVE THE DATASET TO A JSON AND PARQUET FILE##########
    # # s3 = session.resource('s3')
    # my_bucket = s3.Bucket('medispan-etl')
    filename = 'MedispanExportM25Export-Records.json'
    json_buffer = io.StringIO()
    # result = df_merge.to_json(medispanJSONExport, orient="records", lines="true")
    # result = df_merge.to_parquet(medispanparquetExport)
    # result = df_merge.to_json(medispanJSONExport, orient="records")
    
    df_merge.to_json(json_buffer)
    
    s3_client.put_object(Buket='medispan-etl',Key=filename, Body=json_buffer.getvalue())
    
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print("Finished processing at " + current_time)
        
        
    response = {
        "statusCode": 200,
        'body': json.dumps("Code worked!")
    }
    
    return response
    
def Add_A1 (ndc, recordcode, string):
    #A1: Key Identifier Record. Basic Drug Information
    
    #DATA TO GRAB FROM STRING
    global df_a1
    IsMaintenanceDrug = boolCodeReturn(string[68:69].strip())
    routeOfAdmin =  string[71:73].strip()
    #ndcName = string[71:73].strip()
    
    #ADD RECORD TO DATAFRAME
    series = pd.Series (data=[ndc, IsMaintenanceDrug, routeOfAdmin], index=['NDC', 'IsMaintenanceDrug', 'RouteOfAdmin'])
    df_a1 = df_a1.append(series, ignore_index=True)

def Add_G1 (ndc, recordcode, string):
    #G1: Generic Product Identifier (GPI) and GPI Generic Name.
    global df_g1
    
    #DATA TO GRAB FROM STRING
    gpicode = string[16:30].strip()
    gpigenericname = string[35:95].strip()
    
    #ADD RECORD TO DATAFRAME
    series = pd.Series (data=[ndc, gpicode, gpigenericname], index=['NDC', 'GPICode', 'GPIGenericName'])
    df_g1 = df_g1.append(series, ignore_index=True)
       
def Add_J1 (ndc, recordcode, string):
    #The Manufacturer Name (J1) Record is present for each drug product and contains the 
    #manufacturer’s information and product description abbreviations for prescription labels
    
    #DATA TO GRAB FROM STRING
    global df_j1
    ndcName = string[56:81].strip()
    
    #ADD RECORD TO DATAFRAME
    series = pd.Series (data=[ndc, ndcName], index=['NDC', 'NDCName'])
    df_j1 = df_j1.append(series, ignore_index=True)
    
def Add_P01 (ndc, recordcode, string):
    #The Ingredient (P) Record contains ingredient information, such as ingredient name and strength.
    #P01 is the first active ingredient
    global df_p1
    
    #DATA TO GRAB FROM STRING
    genericIngredientName = string[50:90].strip()
        
    #ADD RECORD TO DATAFRAME
    series = pd.Series (data=[ndc, genericIngredientName], index=['NDC', 'GenericIngredientName'])
    df_p1 = df_p1.append(series, ignore_index=True)

    
def Add_R01 (ndc, recordcode, string):
    #The Average Wholesale Price (AWP) Records are present for all drug products on file
    #with AWP information. 
    global df_r1
    
    #DATA TO GRAB FROM STRING
    IsAWP = boolCodeReturn(string[16:17].strip())
    #AWPPackagePrice = string[17:27].lstrip("0")
    AWPUnitPrice = int(string[27:40].lstrip("0"))
    AWPUnitPrice = AWPUnitPrice / 100000
    AWPEffectiveDate = string[40:48]
          
    #ADD RECORD TO DATAFRAME
    series = pd.Series (data=[ndc, AWPUnitPrice, IsAWP, AWPEffectiveDate], index = ['NDC', 'AWPUnitPrice', 'IsAWP', 'AWPEffectiveDate'])
    df_r1 = df_r1.append(series, ignore_index=True)

def Add_Q01 (ndc, recordcode, string):
#The WAC Price Records are present for all drug products on file with WAC 
#information. The Q01 record contains the current WAC information 
    global df_q1

    #DATA TO GRAB FROM STRING
    #AWPPackagePrice = string[17:27].lstrip("0")
    WACUnitPrice = int(string[27:40].lstrip("0"))
    WACUnitPrice = WACUnitPrice / 100000
    WACPEffectiveDate = string[40:48]
            
    #ADD RECORD TO DATAFRAME
    series = pd.Series (data=[ndc, WACUnitPrice, WACPEffectiveDate], index = ['NDC', 'WACUnitPrice', 'WACEffectiveDate'])
    df_q1 = df_q1.append(series, ignore_index=True)
  
  

def boolCodeReturn (code):
    #Medispan uses "X" for bools. This function to change char "X" to a bool
    if code == "X":
        return 1
    else:
        return 0

def errorHandler(ndc, recordcode, string): 
#switch default if record code is not handled via a function. Need to determine what if anything to do 
#with data on here. 
    pass
