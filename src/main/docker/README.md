To build container

docker build -t giovibal/mqtt-broker.

To run container (10.0.0.2 is the host machine, and 10.0.0.1 is another host)

docker run -p 1883-1885:1883-1885 -p 11883-11885:11883-11885 -p 7007:7007 -p 5701:5701 -e PUBLIC_ADDRESS="10.0.0.2:5701" -e CLUSTER_MEMBERS="10.0.0.1 10.0.0.2" --memory=512m --name=mqtt-broker -i -d giovibal/mqtt-broker
