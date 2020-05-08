"""
Module that simulate drone mqtt messages
"""
import time
from enum import Enum

import paho.mqtt.client as mqtt
import numpy as np


class ErrorMessage(Enum):
    """
    Violation message given by drones
    """
    BAD_PARKING_0 = 0
    BAD_PARKING_1 = 1
    BAD_PARKING_2 = 2
    CANT_TAKE_ACTION = 3


DRONE_IDS = np.arange(200)

MY_IP = '192.168.1.54'
client = mqtt.Client('drone')


def on_message(client, userdata, message):
    """
    CallBack function
    """
    print("message received ", str(message.payload.decode("utf-8")))
    print("message topic=", message.topic)
    print("message qos=", message.qos)
    print("message retain flag=", message.retain)


print("creating new instance")
client.on_message = on_message  # attach function to callback
print("connecting to broker")
client.connect(MY_IP)  # connect to broker
while True:
    for ID in DRONE_IDS:
        client.loop_start()
        print("Subscribing to topic", "drones/messages")
        client.subscribe("drones/messages")
        print("Publishing message to topic", "drones/messages")
        client.publish("drones/messages", "{doneId: " + str(ID)
                       + ", message: "
                       + ErrorMessage(np.random.choice(np.arange(4), p=[0.33, 0.33, 0.33, 0.01])).name + "}")
        time.sleep(4)
        client.loop_stop()
