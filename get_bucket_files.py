import boto3
import os

## CONFIGURE YOUR SYSTEM TO ACCESS BUCKET USING ACCESS KEY
#
# Option 1: AWS CLI
#   If you already have the AWS Command Line Interface installed, you can run the following command
#       'aws configure'
#   You should see the following prompts
#        AWS Access Key ID [None]: ACCESS_KEY
#        AWS Secret Access Key [None]: SECRET_KEY
#        Default region name [None]: 
#        Default output format [None]:
#   Enter the Data Bucket Access Key ID and Secret Access Key. Leave the last two prompts empty (hit Enter/Return).
#   If you do not already have AWS CLI installed, you can find more information at https://aws.amazon.com/cli/
#
# Option 2: Credentials File
#   By default, the aws configuration file is at ~/.aws/credentials in your system.
#   In the credentials file, specify the Access Key ID and Secret Access Key for the default profile:
#       [default]
#       aws_access_key_id = ACCESS_KEY
#       aws_secret_access_key = SECRET_KEY
#
# This should be a one-time configuration process to gain access to the Data Bucket.


# Set bucket parameters
BUCKET_NAME = "empatica-us-east-1-prod-data"
PREFIX = "v2/1104/"

def getFiles(outputDir='./avro'):
    # Create s3 resource and bucket object
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(BUCKET_NAME)

    # Iterate through bucket objects
    for my_bucket_object in bucket.objects.filter(Prefix = PREFIX):
        # Operate on only .avro files
        if (my_bucket_object.key.endswith(".avro")):
            # Filepath in bucket
            key = my_bucket_object.key
            filename = my_bucket_object.key.split("/")[-1]
            id = filename.split("_")[0]

            # If id directory doesn't already exist, create it
            idDir = os.path.join(outputDir, id)
            if not os.path.isdir(outputDir):
                os.mkdir(outputDir)
            if not os.path.isdir(idDir):
                os.mkdir(idDir)

            # Download each .avro file (if it doesn't already exist)
            # There is also an option to download as a fileobj, may be more efficient for code integration
            if not os.path.isfile(os.path.join(idDir, filename)):
                print("Downloading: ", os.path.join(idDir, filename))
                bucket.download_file(key, os.path.join(idDir, filename))
            else:
                print("Skipping", os.path.join(idDir, filename), "(file already exists)")


# getFiles()