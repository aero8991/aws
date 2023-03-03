from pyspark.sql import SparkSession
from pyspark.sql.functions import col

S3_DATA_SOURCE_PATH = 's3://drossano-emr-cluster-123/data-source/survey_results_public.csv'
S3_DATA_OUTPUT_PATH  = 's3://drossano-emr-cluster-123/data-output'



def main():
    spark = SparkSession.builder.appName('EMRDemoApp').getOrCreate()
    all_data = spark.read.csv(S3_DATA_SOURCE_PATH, header=True)
    count = all_data.count()
    print(f'Total number of records in the data source: {count}')
    selected_data = all_data.where((col('Country') == 'United States') & (col('MainBranch') =='I code primarily as a hobby'))
    hobby = selected_data.count()
    print(f'The number of workers who are from the US and code as a hobby: {hobby}')
    selected_data.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH)
    print(f'Selected data was saved to the s3 bucket: {S3_DATA_OUTPUT_PATH}')
    df = spark.read.csv(S3_DATA_SOURCE_PATH, inferSchema=True, header=True)
    df.show(2)
    df.createOrReplaceGlobalTempView('filter_view')
    df_filter = spark.sql("""select * from fiter_view where Country = 'United States'""" )
    df_filter.show()

if __name__ == '__main__':
    main()
