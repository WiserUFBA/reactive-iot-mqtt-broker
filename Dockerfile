FROM java:8

WORKDIR /opt

## INSTALL MAVEN
#RUN mkdir src
#WORKDIR src
#ENV MAVEN_PACKAGE=apache-maven-3.3.9
#RUN wget http://mirrors.muzzy.it/apache/maven/maven-3/3.3.9/binaries/$MAVEN_PACKAGE-bin.tar.gz
#RUN tar xzvf $MAVEN_PACKAGE-bin.tar.gz

## COMPILE
#ADD . .
#RUN $MAVEN_PACKAGE/bin/mvn -DskipTests clean package

## SETUP
WORKDIR /opt
#RUN cp src/target/vertx-mqtt-broker-mod-2.2-SNAPSHOT-fat.jar mqtt-broker.jar
ADD target/vertx-mqtt-broker-mod-2.2-SNAPSHOT-fat.jar mqtt-broker.jar
ADD config.json config.json

## CLEANUP
#RUN rm -Rf ~/.m2
#RUN rm -Rf src

## EXECUTE COMMAND (without parameters)
ENTRYPOINT ["java", "-jar", "-XX:OnOutOfMemoryError=\"kill -9 %p\"", "-XX:+UseG1GC", "mqtt-broker.jar"]
