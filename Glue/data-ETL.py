try:
    import sys
    import boto3
    import pandas as pd
    import logging
    import time
    import io
    from functools import reduce
    print("imports are successfull! ")
except Exception as e: 
    print(e)
    


# root = logging.getLogger()
# root.setLevel(logging.DEBUG)

# handler = logging.StreamHandler(sys.stdout)
# handler.setLevel(logging.DEBUG)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# handler.setFormatter(formatter)
# root.addHandler(handler)
# root.info("check")

s3job = boto3.resource('s3')
s3 = boto3.client('s3')
data1 = s3.get_object(Bucket='medispan-etl', Key='DataSource/M25-small-test.txt')
data2 = s3.get_object(Bucket='medispan-etl', Key='DataSource/M25')

##########STEP 1: create data frames for each type of data##########
df_a1 = pd.DataFrame(columns = ['NDC', 'IsMaintenanceDrug', 'RouteOfAdmin'])
df_g1 = pd.DataFrame(columns = ['NDC', 'GPICode', 'GPIGenericName'])
df_j1 = pd.DataFrame(columns = ['NDC', 'NDCName'])
df_p1 = pd.DataFrame(columns = ['NDC', 'GenericIngredientName'])
df_q1 = pd.DataFrame(columns = ['NDC', 'WACUnitPrice', 'WACEffectiveDate'])
df_r1 = pd.DataFrame(columns = ['NDC', 'AWPUnitPrice', 'IsAWP', 'AWPEffectiveDate'])
print("got dataframes!")


file_lines = data1['Body'].iter_lines()
for file in file_lines:
    print(file)

    
    
txt_file = s3job.Object('medispan-etl', 'DataSource/M25-small-test.txt').get()['Body'].read().decode('utf-8').splitlines()
print(txt_file,"worked!!!")

 
# medispanSource = open(r"C:\Users\drossano\OneDrive - Genoa Healthcare\Documents\POC\MedispanExport\M25", "r")
# medispanJSONExport = (r"C:\Users\drossano\OneDrive - Genoa Healthcare\Documents\POC\MedispanExportM25Export-Records.json")
# medispanparquetExport = (r"C:\Users\drossano\OneDrive - Genoa Healthcare\Documents\POC\MedispanExport\M25Export.parquet")

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




##########STEP 2: Loop through file by reading each line##########
# fileRow = medispanSource.readline()
i= 0
while i < len(txt_file):
    for f in txt_file:
    # while fileRow: 
            
        currentString = f
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
        i += 1



##########STEP 3: JOIN THE DATA TOGETHER##########
df_merge = pd.merge(df_a1, df_g1, how="left", on="NDC")
df_merge = pd.merge(df_merge, df_j1, how="left", on="NDC")
df_merge = pd.merge(df_merge, df_p1, how="left", on="NDC")
df_merge = pd.merge(df_merge, df_q1, how="left", on="NDC")
df_merge = pd.merge(df_merge, df_r1, how="left", on="NDC")

##########STEP 4: SAVE THE DATASET TO A JSON AND PARQUET FILE##########
# result = df_merge.to_json(medispanJSONExport, orient="records", lines="true")
# result = df_merge.to_parquet(medispanparquetExport)
# result = df_merge.to_json(medispanJSONExport, orient="records")

df_list=[df_a1,df_g1,df_j1, df_p1, df_q1,df_r1 ]
cols = ['NDC', 'GPICode']
df_final = pd.concat(df_list,ignore_index=True)

df_ok_final = reduce(lambda  left,right: pd.merge(left,right,on=['NDC'],
                                            how='outer'), df_list)

#view results
print(df_merge)
print(df_final)
print(df_ok_final)

output_file = df_ok_final.to_json(orient="records", lines="true")
print('saved json')

output_file_parquet = df_ok_final.to_parquet()
print('saved parquet')

s3.put_object(Body=output_file, Bucket='medispan-etl', Key='output/M25Export-Records.json')

s3.put_object(Body=output_file_parquet, Bucket='medispan-etl', Key='output/M25Export-Records.parquet')
print('saved file to s3')
