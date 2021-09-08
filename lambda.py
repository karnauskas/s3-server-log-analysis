import json
import boto3
import re
import os

def lambda_handler(event, context):
    # TODO implement
    #bucketName = 'picomy-serveraccesslog'
    #salogTableName = 'default.salog_rawdata_3'
    #athena_cache = 's3://athenaquery'
    bucketName = os.getenv('bucketName')
    salogTableName = os.getenv('salogTableName')
    athena_cache = os.getenv('AthenaCache')
    
    client = boto3.client('s3')
    l1_prefix = client.list_objects_v2(Bucket = bucketName, Delimiter = '/', Prefix = 'reqbucket=')
    if l1_prefix['KeyCount'] > 0:
        for ds_prefix in l1_prefix['CommonPrefixes']:
            ds = ds_prefix['Prefix']
            print("--------------------%s--------------------"%ds)
            l2_prefix = client.list_objects_v2(Bucket = bucketName, Delimiter = '/', Prefix = ds_prefix['Prefix'])
            #print(l2_prefix)
            if l2_prefix.get('Contents',False):
                for log in l2_prefix['Contents']:
                    log_name = log['Key']
                    #print(log_name)
                    match = re.search('(?P<requestedbucket>reqbucket=.*)/(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})-(?P<hour>\d{2})',log_name,re.ASCII)
                    match1 = re.search('reqbucket=.*/(?P<logshortname>\d{4}-.*)', log_name, re.ASCII)
                    if match and match1:
                        print(match.group('requestedbucket'))
                        requested_bucket=match.group('requestedbucket')
                        print(match.group('year'))
                        requested_year='reqyear=' + match.group('year')
                        print(match.group('month'))
                        requested_month='reqmonth='+ match.group('month')
                        print(match.group('day'))
                        requested_day='reqday='+ match.group('day')
                        print(match.group('hour'))
                        requested_hour='reqhour='+ match.group('hour')
    
                        print(match1.group('logshortname'))
                        log_short_name=match1.group('logshortname')
                        # copy object
                        client.copy({'Bucket': bucketName,'Key': log_name}, Bucket = bucketName, Key = requested_bucket + '/' + requested_year + '/' + requested_month + '/' + requested_day + '/' + requested_hour + '/' + log_short_name)
                        # delete object
                        client.delete_object(Bucket = bucketName, Key = log_name)
                print('--------------------------------------------------------')
    
    # Refresh partitions
    client1 = boto3.client('athena')
    rsp=client1.start_query_execution(QueryString='msck repair table '+ salogTableName, ResultConfiguration={'OutputLocation': athena_cache})
    #print(rsp)
    
    query_execution_id = rsp["QueryExecutionId"]
    
    # Query in progress
    print("Refresh Partition", end="", flush=True)
    while 1:
        print(".", end="", flush=True)
        query_status = client1.get_query_execution(QueryExecutionId=query_execution_id)
    
        #print(query_status["QueryExecutions"][0]["Status"]["State"])
        if query_status["QueryExecution"]["Status"]["State"] == "SUCCEEDED":
            print("\nDone",flush=True)
            break
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
