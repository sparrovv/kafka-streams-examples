import os
import json


def contact_detail():
    return {
        "id": "uid1",
        "name": "Michal",
        "telephoneNumber": "+48324234"
    }


def quote_created():
    return {
        "eventId": "qc1",
        "reference": "ref1",
        "userId": "uid1",
        "quotesNumber": 3
    }


events = [
    ("quotes_created_3p", json.dumps(quote_created()), "uid1"),
    ("contact_details_2p", json.dumps(contact_detail()), "uid1"),
]


def kafkaProducerCmd(topic: str, msg: str, key=None):
    if key != None:
        return f"echo '{key}::{msg}' | kafka-console-producer --broker-list localhost:9092 --topic {topic} --property " \
               f"'key.separator=::' --property 'parse.key=true' "
    else:
        return f"echo '{msg}' | kafka-console-producer --broker-list localhost:9092 --topic {topic}"


cmds = []
for e in events:
    if len(e) == 2:
        cmds.append(kafkaProducerCmd(e[0], e[1]))
    else:
        cmds.append(kafkaProducerCmd(e[0], e[1], e[2]))

for c in cmds:
    print(c)
    os.system(c)
