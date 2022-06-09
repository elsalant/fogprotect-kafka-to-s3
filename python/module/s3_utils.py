import boto3
import uuid
import tempfile
import time

BUCKET_PREFIX = '-fogprotect-'
SEED = 'sm' # for file name

class S3utils:

    def __init__(self, logger, s3_access_key,s3_secret_key, s3_URL):
        self.connection = boto3.resource(
            's3',
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
            endpoint_url=s3_URL
        )
        self.logger = logger

    def contentToFile(self, content):
        random_file_name = ''.join([str(uuid.uuid4().hex[:6]), SEED])
        try:
            tempFile = tempfile.NamedTemporaryFile(prefix=random_file_name, suffix=".json", mode='w+t')
            tempFile.write(content)
            tempFile.seek(0)
        except Exception as e:
            tempFile.close()
            print("exception writing to tmpfile")
            print(e)
        return tempFile

    def write_to_S3(self, bucketName, data):
        matchingBucket = self.get_resource_buckets(bucketName)
        if len(matchingBucket) > 1:
            raise AssertionError('Too many matching buckets found! ' + len(matchingBucket) + ' ' + str(matchingBucket))
        elif len(matchingBucket) == 1:
            bucketName = matchingBucket[0]
            self.logger.info(f'matching bucket found: ' + bucketName)
        else:
            bucketName, response = self.create_bucket(bucketName, self.connection)
            self.logger.info(f"new bucket being created:" + bucketName)
        tempFile = self.contentToFile(data)
        # Generate a random prefix to the resource type
        fName = ''.join([str(uuid.uuid4().hex[:6]), SEED])
        self.write_to_bucket(bucketName, tempFile, fName)
        self.logger.info(f"information written to bucket ", bucketName, ' as ', fName)
        return None

    def write_to_bucket(self, bucketName, tempFile, fnameSeed):
        try:
            bucketObject = self.connection.Object(bucket_name=bucketName, key=fnameSeed)
            self.logger.info(f"about to write to S3: bucketName = " + bucketName + " fnameSeed = " + fnameSeed)
            bucketObject.upload_file(tempFile.name)
        finally:
            tempFile.close()
        return None

    def create_bucket(self, bucket_name, s3_connection):
        session = boto3.session.Session()
        current_region = session.region_name
        if current_region == None:
            current_region = ''
        bucket_response = s3_connection.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
            'LocationConstraint': current_region})
        self.logger.info(bucket_name, current_region)
        return bucket_name, bucket_response

    def get_resource_buckets(self, searchPrefix):
        # Get a bucket with a name that contains the passed prefix
        try:
            bucketCollection = self.connection.buckets.all()
        except:
            self.logger.WARN('connection.buckets.all() fails!')
            time.sleep(600)   # REMOVE THIS - debugging aid only
        bucketList = []
        for bucket in bucketCollection:
            bucketList.append(str(bucket.name))
        matchingBuckets = [s for s in bucketList if searchPrefix in s]
        if (matchingBuckets):
            self.logger.info("matchingBuckets = " + str(matchingBuckets))
        return matchingBuckets