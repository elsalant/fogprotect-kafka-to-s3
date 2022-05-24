from kubernetes import client, config
import yaml
import base64
import logging
import os

from module.s3_utils import S3utils
from module.kafka_utils import KafkaUtils
from module.policyUtils import PolicyUtils

FLASK_PORT_NUM = 5559  # this application

ACCESS_DENIED_CODE = 403
ERROR_CODE = 406
VALID_RETURN = 200

TEST = True   # allows testing outside of Fybrik/Kubernetes environment

FIXED_SCHEMA_ROLE = 'missing role'
FIXED_SCHEMA_ORG  = 'missing org'

logger = logging.getLogger(__name__)
cmDict = {}

def getSecretKeys(secret_name, secret_namespace, safe_bucket, unsafe_bucket ):
    try:
        config.load_incluster_config()  # in cluster
    except:
        config.load_kube_config()   # useful for testing outside of k8s
    v1 = client.CoreV1Api()
    if TEST == False:
        secret = v1.read_namespaced_secret(secret_name, secret_namespace)
        accessKeyID = base64.b64decode(secret.data['access_key'])
        secretAccessKey = base64.b64decode(secret.data['secret_key'])
        safeBucket = base64.b64decode(secret.data['safe_bucket']),
        unsafeBucket = base64.b64decode(secret.data['unsafe_bucket'])
        return(accessKeyID.decode('ascii'), secretAccessKey.decode('ascii'),
               safeBucket.decode('ascii'), unsafeBucket.decode('ascii'))
    else:
        return("change-me", "change-me", "change-me", "change-me")

def readConfig(CM_PATH):
    if not TEST:
        try:
            with open(CM_PATH, 'r') as stream:
                cmReturn = yaml.safe_load(stream)
        except Exception as e:
            raise ValueError('Error reading from file! ', CM_PATH)
    else:
        cmDict = {'WP2_TOPIC': 'fhir-wp2', 'HEIR_KAFKA_HOST': 'kafka.fybrik-system:9092', 'VAULT_SECRET_PATH': None,
                  'SECRET_NSPACE': 'fybrik-system', 'SECRET_FNAME': 'credentials-els',
                  'S3_URL': 'http://s3.eu.cloud-object-storage.appdomain.cloud', 'SUBMITTER': 'EliotSalant',
                  'SAFE_BUCKET':'safe_bucket', 'UNSAFE_BUCKET':'unsafe_bucket'}
        return(cmDict)
    cmDict = cmReturn.get('data', [])
    logger.info(f'cmReturn = ', cmReturn)
    return(cmDict)

def main():
    global cmDict
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info(f"starting module!!")

    CM_PATH = '/etc/conf/conf.yaml' # from the "volumeMounts" parameter in templates/deployment.yaml

    kafkaUtils = KafkaUtils(logger)
    policyUtils = PolicyUtils(logger)
    s3Utils = S3utils(logger)
    cmDict = readConfig(CM_PATH)

    # Get the secrets for S3 connection and then fire up the S3 object
    secret_namespace = cmDict['SECRET_NSPACE']
    secret_fname = cmDict['SECRET_FNAME']
    safe_bucket = cmDict['SAFE_BUCKET']
    unsafe_bucket = cmDict['UNSAFE_BUCKET']
    s3_URL = cmDict['S3_URL']
    keyId, secretKey, safeBucket, unsafeBucket = getSecretKeys(secret_fname, secret_namespace, safe_bucket, unsafe_bucket)
    S3utils(logger, keyId, secretKey, safeBucket, unsafeBucket, s3_URL)

# Listen on the Kafka queue for ever. When a message comes in, determine the "Status" env and write to S3 bucket accordingly
    for message in kafkaUtils.consumer:
        messageDict = message.value
        filteredData = policyUtils.apply_policy(messageDict)
# The environment variable, SITUATION_STATUS, is created from a config map and can be externally changed.
# The value of this env determines to which bucket to write
        situationStatus = os.getenv('SITUATION_STATUS')
        assert(situationStatus)
        if situationStatus.lower() == 'safe':
            bucketName = safeBucket
        elif situationStatus.lower() == 'unsafe':
            bucketName = unsafeBucket
        else:
            raise Exception('situationStatus = '+situationStatus)
        s3Utils.write_to_S3(bucketName, filteredData)

if __name__ == "__main__":
    main()