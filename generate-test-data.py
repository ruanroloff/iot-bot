#!/usr/bin/python

import sys
import ssl
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import json
import random
import time
from datetime import datetime, timedelta
import math
import subprocess as sp

#Setup our MQTT client and security certificates
#Make sure your certificate names match what you downloaded from AWS IoT

#Retrieve your unique IoT endpoint
ENDPOINT = 'yourendpoint.eu-west-1.amazonaws.com'
CLIENT_ID = 'yourclientid'
TOPIC = 'your/topic'
PORT = 8883

# constants
PUBLISH_INTERVAL_SECONDS = 5
MAX_PUBLISH_COUNT = 1440 #publish for 2 hours if not aborted
SAMPLES_PER_DAY = 3600 * 24 / PUBLISH_INTERVAL_SECONDS
HOURS_FROM_UTC = -7
# Geohash: 'Chicago', 'Detroit'
LOCATION = ['dp3wjztvtwq8w', 'dpsby4pymtvtu']
TEMP_MEDIAN = 38
HUMIDITY_MEDIAN = 50
PRESSURE_MEDIAN = 29
AMPLITUDE = 1

# generate data centered at median with a sine wave of amplitude 
def GenerateData(currentSamplePoint, deviceID):
    data = {}

    rads = 2 * math.pi * currentSamplePoint / SAMPLES_PER_DAY
    new_temp = 0 
    # offset one set of data by 90 degrees, so the data plots don't completely obscure each other
    if deviceID == 18:
        rads += math.pi / 2
    
    #add random values to temperature
    if deviceID == 18:
        new_temp = random.randint(1, 8) 
    else:
        new_temp = random.randint(1, 4)
        
    offset = AMPLITUDE * math.sin(rads)
    data['temperature'] = TEMP_MEDIAN + offset + new_temp
    data['humidity'] = HUMIDITY_MEDIAN + offset
    data['pressure'] = PRESSURE_MEDIAN + offset
    return data

# generate metadata and timestamps
def GeneratePayload(messageTime, deviceID, location, currentSamplePoint):
    payloadDict = {}
    # add a second to the second device's timestamp so the timestamp for each device are not the same
    if (deviceID == 18):
        messageTime += timedelta(seconds = 1)
    timestamp = messageTime
    # get measurements
    data = GenerateData(currentSamplePoint, deviceID)
    # timestamp needs to be a whole number - timestamps don't like 1.23456+18
    payloadDict['timestamp'] = int(timestamp.timestamp())
    # pass a copy of the local time because I can't convert nanosecond UTC timestamps into local time in my head
    payloadDict['local_time'] = (messageTime + timedelta(hours = HOURS_FROM_UTC)).isoformat()
    # a couple more pieces of metadata
    payloadDict['deviceID'] = deviceID
    payloadDict['location'] = location
    # actual measurements inserted
    payloadDict['temperature'] = data['temperature']
    payloadDict['pressure'] = data['pressure']
    payloadDict['humidity'] = data['humidity']
    return payloadDict

# one complete sine wave every day
currentSamplePoint = 1
# send two messages to represent two different devices
count = 1

#Create an MQTT client
mqttc = AWSIoTMQTTClient(CLIENT_ID)

mqttc.configureEndpoint(ENDPOINT,PORT)
#Create credentials by locating your certificates and private key
mqttc.configureCredentials("./cert/rootCA.pem","./cert/privateKey.pem","./cert/certificate.pem")

#Convert a tuple to JSON
def json_encode(message):
        return json.dumps(message)

mqttc.json_encode=json_encode

#Connect to the MQTT broker
mqttc.connect()
print("Connected")

publish_count = 0

while publish_count < MAX_PUBLISH_COUNT:
    messageTime = datetime.utcnow()
    for i in range(2):
        #create new deviceid from offset
        z_deviceID = i + 18
        
        payloadDict = GeneratePayload(messageTime, z_deviceID, LOCATION[i], currentSamplePoint)
        payload = mqttc.json_encode(payloadDict)
        mqttc.publish(TOPIC, payload, 0)
        print('added[{0}]: {1}'.format(count, payload))
        count += 1
    publish_count += 1
    # need a modulo counter that goes from [0,number of samples per day)
    currentSamplePoint += 1
    currentSamplePoint %= SAMPLES_PER_DAY
    time.sleep(PUBLISH_INTERVAL_SECONDS)

mqttc.disconnect()
