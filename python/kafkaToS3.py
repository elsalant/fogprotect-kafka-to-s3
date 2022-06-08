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

TEST = False   # allows testing outside of Fybrik/Kubernetes environment

FIXED_SCHEMA_ROLE = 'missing role'
FIXED_SCHEMA_ORG  = 'missing org'

logger = logging.getLogger(__name__)
cmDict = {}

def getSecretKeys(secret_name, secret_namespace):
    try:
        config.load_incluster_config()  # in cluster
    except:
        config.load_kube_config()   # useful for testing outside of k8s
    v1 = client.CoreV1Api()
    if TEST == False:
        logger.info('secret_name = ' + secret_name + ' secret_namespace = ' + secret_namespace)
        secret = v1.read_namespaced_secret(secret_name, secret_namespace)
        accessKeyID = base64.b64decode(secret.data['access_key'])
        secretAccessKey = base64.b64decode(secret.data['secret_key'])
        return(accessKeyID.decode('ascii'), secretAccessKey.decode('ascii'))
    else:
        return("change-me", "change-me")

def readConfig(CM_PATH):
    if not TEST:
        try:
            with open(CM_PATH, 'r') as stream:
                cmReturn = yaml.safe_load(stream)
        except Exception as e:
            raise ValueError('Error reading from file! ', CM_PATH)
    else:
        cmDict = {'MSG_TOPIC': 'sm', 'HEIR_KAFKA_HOST': 'kafka.fybrik-system:9092', 'VAULT_SECRET_PATH': None,
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

    cmDict = readConfig(CM_PATH)

    # Get the connection details for S3 connection and then fire up the S3 object
    secret_namespace = cmDict['SECRET_NSPACE']
    secret_fname = cmDict['SECRET_FNAME']
    safeBucketName = cmDict['SAFE_BUCKET']
    unsafeBucketName = cmDict['UNSAFE_BUCKET']
    msg_topic = cmDict['MSG_TOPIC']
    logger.info('secret_namespace = ' + str(secret_namespace) + ' secret_fname = ' + str(secret_fname) +
                ' safeBucketName = ' + str(safeBucketName) + ' unsafeBucketName = ' + str(unsafeBucketName))

    s3_URL = cmDict['S3_URL']
    keyId, secretKey = getSecretKeys(secret_fname, secret_namespace)
    s3Utils = S3utils(logger, keyId, secretKey, s3_URL)
    kafkaUtils = KafkaUtils(logger, msg_topic)
    policyUtils = PolicyUtils(logger)

# Listen on the Kafka queue for ever. When a message comes in, determine the "Status" env and write to S3 bucket accordingly
    for message in kafkaUtils.consumer:
        messageDict = message.value
        logger.info('Read from Kafka: ' + messageDict)
        filteredData = policyUtils.apply_policy(messageDict)
# The environment variable, SITUATION_STATUS, is created from a config map and can be externally changed.
# The value of this env determines to which bucket to write
        situationStatus = os.getenv('SITUATION_STATUS')
        if TEST:
            situationStatus = 'safe'
        assert(situationStatus)
        if situationStatus.lower() == 'safe':
            bucketName = safeBucketName
        elif situationStatus.lower() == 'unsafe':
            bucketName = unsafeBucketName
        else:
            raise Exception('situationStatus = '+situationStatus)
        s3Utils.write_to_S3(bucketName, filteredData)

if __name__ == "__main__":
    main()