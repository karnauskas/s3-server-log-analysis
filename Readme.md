# Introduction
The customer usually records the accessing behavior by enabling CloudTrail data event or S3 bucket server log. S3 bucket server log will be store as none hierarchy layout. When analyze the S3 bucket server log by Athena, the response time usually is slow. This code snippet provides a optimization method.

# Optimization Method
When enable the S3 server log, the log file will be stored as none hierarchy layout.

![](./img/original.svg)

First, when enable the S3 server log, add the prefix as the following:

s3://picomy-serveraccesslog/reqbucket=airports/

![](./img/re-org.svg)

Improve the Athena query speed, the best practice is to partition the S3 server log data. Please deploy the lambda to re-organize the S3 server log to be compatible with Hive partition specification. The partitions will be loaded by the lambda function.

![](./img/hive-compatible.svg)

# Script excution order

- salog.sql
- lambda.py

Lambda Environment variables
  - bucketName, the bucket stores the S3 server log.
  - salogTableName, schema.tableName.
  - AthenaCache, the bucket for Athena query cache, the format s3://bucket-name/.
