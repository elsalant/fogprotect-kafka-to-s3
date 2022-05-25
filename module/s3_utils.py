import boto3
import uuid
import tempfile

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
            writeLine = str(content)
            tempFile = tempfile.NamedTemporaryFile(prefix=random_file_name, suffix=".csv", mode='w+t' )
            self.logging.INFO("Created file is:", tempFile.name)
            tempFile.write(writeLine)
            tempFile.seek(0)   # Rewind - is this necessary?
        except:
            tempFile.close()
            raise IOError("error writing content to temp file")
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
        self.logger.info(f"fName = " + fName + "patientId = " + SEED)
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

    def create_bucket(self, bucket_prefix, s3_connection):
        session = boto3.session.Session()
        current_region = session.region_name
        if current_region == None:
            current_region = ''
        bucket_name = self.create_bucket_name(bucket_prefix)
        bucket_response = s3_connection.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
            'LocationConstraint': current_region})
        self.logger.info(bucket_name, current_region)
        return bucket_name, bucket_response

    def create_bucket_name(self, bucket_prefix):
        # The generated bucket name must be between 3 and 63 chars long
        return ''.join([bucket_prefix, str(uuid.uuid4())])

    def get_resource_buckets(self, searchPrefix):
        # Get a bucket with a name that contains the passed prefix
        bucketCollection = self.connection.buckets.all()
        bucketList = []
        for bucket in bucketCollection:
            bucketList.append(str(bucket.name))
        matchingBuckets = [s for s in bucketList if searchPrefix in s]
        if (matchingBuckets):
            print("matchingBuckets = " + str(matchingBuckets))
        return matchingBuckets