import boto3


def lambda_handler(event, context):
    glue_jobs =	{
        "stores.json": "storesjsonjob",
        "stores.csv": "storesjob",
        "holidays_events.csv": "holidayseventscsvjob",
        "items.csv": "itemscsvjob",
        "transactions.csv":"transactionscsvjob",
        "events.csv":"eventscsvjob"
        }
        
    file_name = event['Records'][0]['s3']['object']['key']
    bucketName=event['Records'][0]['s3']['bucket']['name']
    print("File Name : ",file_name)
    print("Bucket Name : ",bucketName)
    glue=boto3.client('glue');
    
    if file_name == "stores.json":
        response = glue.start_job_run(JobName = glue_jobs[file_name])
        print("Run {}".format(glue_jobs[file_name]))
    
    elif file_name == "stores.csv":
        glue=boto3.client('glue');
        response = glue.start_job_run(JobName = glue_jobs[file_name])
        print("Run {}".format(glue_jobs[file_name]))
    
    elif file_name == "holidays_events.csv":
        glue=boto3.client('glue');
        response = glue.start_job_run(JobName = glue_jobs[file_name])
        print("Run {}".format(glue_jobs[file_name]))
        
    elif file_name == "items.csv":
        glue=boto3.client('glue');
        response = glue.start_job_run(JobName = glue_jobs[file_name])
        print("Run {}".format(glue_jobs[file_name]))
    
    elif file_name == "transactions.csv":
        glue=boto3.client('glue');
        response = glue.start_job_run(JobName = glue_jobs[file_name])
        print("Run {}".format(glue_jobs[file_name]))
    
    elif file_name == "events.csv":
        glue=boto3.client('glue');
        response = glue.start_job_run(JobName = glue_jobs[file_name])
        print("Run {}".format(glue_jobs[file_name]))