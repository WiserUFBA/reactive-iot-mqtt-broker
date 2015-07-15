#!/bin/bash

docker run -p 1883-1885:1883-1885 -p 11883-11885:11883-11885 -p 7007:7007 --memory=512m --name=mqtt-broker -d -t giovibal/mqtt-broker
