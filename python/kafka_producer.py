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

outString = """{
    "RobotAcceleration": {
        "sensors": [
            {
                "arm": "right",
                "axis": 1,
                "value": 0.882
            },
            {
                "arm": "right",
                "axis": 2,
                "value": 0.225
            },
            {
                "arm": "right",
                "axis": 3,
                "value": 0.095
            },
            {
                "arm": "right",
                "axis": 4,
                "value": 0.874
            },
            {
                "arm": "right",
                "axis": 5,
                "value": 0.412
            },
            {
                "arm": "right",
                "axis": 6,
                "value": 0.905
            },
            {
                "arm": "left",
                "axis": 1,
                "value": 0.665
            },
            {
                "arm": "left",
                "axis": 2,
                "value": 0.223
            },
            {
                "arm": "left",
                "axis": 3,
                "value": 0.973
            },
            {
                "arm": "left",
                "axis": 4,
                "value": 0.513
            },
            {
                "arm": "left",
                "axis": 5,
                "value": 0.259
            },
            {
                "arm": "left",
                "axis": 6,
                "value": 0.084
            }
        ]
    },
    "RobotConnection": {
        "connected": true
    },
    "RobotJobs": [
        {
            "arm": "right",
            "job": "FALSE"
        },
        {
            "arm": "left",
            "job": "FALSE"
        }
    ],
    "RobotJointAngle": {
        "sensors": [
            {
                "arm": "right",
                "axis": 1,
                "value": 0.00041
            },
            {
                "arm": "right",
                "axis": 2,
                "value": -129.99992
            },
            {
                "arm": "right",
                "axis": 3,
                "value": 30.00097
            },
            {
                "arm": "right",
                "axis": 4,
                "value": -0.00102
            },
            {
                "arm": "right",
                "axis": 5,
                "value": 40.00035
            },
            {
                "arm": "right",
                "axis": 6,
                "value": 0.00008
            },
            {
                "arm": "left",
                "axis": 1,
                "value": 0.024
            },
            {
                "arm": "left",
                "axis": 2,
                "value": 0.729
            },
            {
                "arm": "left",
                "axis": 3,
                "value": 0.106
            },
            {
                "arm": "left",
                "axis": 4,
                "value": 0.545
            },
            {
                "arm": "left",
                "axis": 5,
                "value": 0.486
            },
            {
                "arm": "left",
                "axis": 6,
                "value": 0.613
            }
        ]
    },
    "RobotSpeed": {
        "sensors": [
            {
                "arm": "right",
                "axis": 1,
                "value": 0.2
            },
            {
                "arm": "right",
                "axis": 2,
                "value": 0.06681
            },
            {
                "arm": "right",
                "axis": 3,
                "value": -0.033
            },
            {
                "arm": "right",
                "axis": 4,
                "value": 0.000
            },
            {
                "arm": "right",
                "axis": 5,
                "value": 0.053
            },
            {
                "arm": "right",
                "axis": 6,
                "value": -0.052
            },
            {
                "arm": "left",
                "axis": 1,
                "value": 0.505
            },
            {
                "arm": "left",
                "axis": 2,
                "value": 0.793
            },
            {
                "arm": "left",
                "axis": 3,
                "value": 0.853
            },
            {
                "arm": "left",
                "axis": 4,
                "value": 0.489
            },
            {
                "arm": "left",
                "axis": 5,
                "value": 0.014
            },
            {
                "arm": "left",
                "axis": 6,
                "value": 0.503
            }
        ]
    },
    "RobotTorq": {
        "sensors": [
            {
                "arm": "right",
                "axis": 1,
                "value": 0
            },
            {
                "arm": "right",
                "axis": 2,
                "value": -4.940
            },
            {
                "arm": "right",
                "axis": 3,
                "value": 4.093
            },
            {
                "arm": "right",
                "axis": 4,
                "value": 0.977
            },
            {
                "arm": "right",
                "axis": 5,
                "value": -0.031
            },
            {
                "arm": "right",
                "axis": 6,
                "value": 0.133
            },
            {
                "arm": "left",
                "axis": 1,
                "value": 0.463
            },
            {
                "arm": "left",
                "axis": 2,
                "value": 0.386
            },
            {
                "arm": "left",
                "axis": 3,
                "value": 0.703
            },
            {
                "arm": "left",
                "axis": 4,
                "value": 0.181
            },
            {
                "arm": "left",
                "axis": 5,
                "value": 0.125
            },
            {
                "arm": "left",
                "axis": 6,
                "value": 0.48
            }
        ]
    },
    "RobotWarehouse": {
        "inventory": [
            {
                "count": "FALSE",
                "type": "body",
                "value": 0
            },
            {
                "count": "FALSE",
                "type": "lid",
                "value": 2
            },
            {
                "count": "FALSE",
                "type": "pcb",
                "value": 2
            }
        ]
    },
    "timestamp": "2022-07-20 12:08:57"
}"""

#outString = '{"DOB": "01/02/1988", "FirstNAME": "' + FNAMES[randFname] + '", "LastNAME": "'  + LNAMES[randLname] + '"}'
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