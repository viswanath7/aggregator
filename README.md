## Local execution 
Run the only class `com.example.spark.Application` and watch the console for output
## EMR Deployment
In order to submit the spark job to a spark cluster on Amazon web service's (AWS) elastic map reduce (EMR), follow the steps listed below
 - Login to your account in AWS and create a EMR cluster selecting the appropriate version of spark. This application uses version 3.0.1 of spark
 - Once the cluster is provisioned successfully, SSH into the master node using its external DNS name that's displayed in AWS.
    - One would typically need to provide private key for authentication 
    - Ensure that access to SSH port is allowed for your IP address to carry this operation.
 - Use SBT's assembly plugin to create a fat jar that would bundle along dependencies
    - Before creating the artifact, ensure that spark configuration (such as executors) is not specified in the application code, as it cannot be over-ridden via command line or configuration files.
    - In the application, use `s3n://` URLs, when specifying file paths.
 - Place script and data files in a location such as S3 so that EMR can easily access them. The file can be easily copied as shown  
   `aws s3 cp s3://bucket-name/file-name ./ `
   - Ensure that the files carry correct permissions so that access to them are permitted 
 - Run `spark-submit <spark-script>.jar`
 
 