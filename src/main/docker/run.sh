#!/bin/bash

docker run -p 1883-1884:1883-1884 -p 11883-11884:11883-11884 --memory=512m --name=mqtt-broker -d -t giovibal/mqtt-broker
