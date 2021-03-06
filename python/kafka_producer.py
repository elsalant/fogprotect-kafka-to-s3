from kafka import KafkaProducer
from random import seed
from random import randint
from json import dumps

TEST = False
if TEST:
    KAFKA_HOST = 'localhost:9092'
else:
    KAFKA_HOST = 'kafka.fybrik-system:9092'
KAFKA_TOPIC = 'sm'

FNAMES = ['Jim', 'John', 'Joan', 'Jack']
LNAMES = ['Smith', 'Jones', 'Parker', 'Henderson']

seed(1)
randFname = randint(0, len(FNAMES) -1 )
randLname = randint(0, len(LNAMES) - 1)

outString = '{"DOB": "01/02/1988", "FirstNAME": "' + FNAMES[randFname] + '", "LastNAME": "'  + LNAMES[randLname] + '"}'
print("about to connect to " + KAFKA_HOST + ' writing to topic ' + KAFKA_TOPIC)
try:
    producer = KafkaProducer(bootstrap_servers=[KAFKA_HOST],
                             value_serializer=lambda x:
                             dumps(x).encode('utf-8'))
except Exception as e:
    print('Connecting to Kafka failed!')
    print(e)
try:
    producer.send(KAFKA_TOPIC, value=outString)
    producer.flush()
except Exception as e:
    print("Error sending "+outString+" to Kafka")
    print(e)

print(outString + ' sent to Kafka topic ' + KAFKA_TOPIC + ' at ' + KAFKA_HOST)
exit(0)