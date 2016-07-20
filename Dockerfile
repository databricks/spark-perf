FROM fedora

### this dockerfile is build to run spark-perf.
### adopted from https://github.com/fedora-cloud/Fedora-Dockerfiles
### we additionally install python-pip and argparse for spark 

RUN dnf -y update && dnf clean all
RUN dnf -y install python-pip tar gzip 
RUN dnf -y install java java-devel 
RUN dnf -y install python && dnf clean all
RUN easy_install argparse
# set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-openjdk
# set installed Maven version
ENV MAVEN_VERSION 3.2.5
# Download and install Maven
RUN curl -sSL http://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz | tar xzf - -C /usr/share \
&& mv /usr/share/apache-maven-$MAVEN_VERSION /usr/share/maven \
&& ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

ENV M2_HOME /usr/share/maven

### Now we setup the spark-perf parts.
RUN dnf install -y unzip git wget
RUN mkdir -p /root/spark-ec2/ && chmod -R 777 /root/spark-ec2/
RUN cd /opt/ && git clone https://github.com/paulp/sbt-extras.git  && chmod 777 sbt-extras/* 
RUN ls -altrh /opt/sbt-extras/
RUN dnf install -y which

### wget spark client so we can have spark-submit 
WORKDIR /opt/
RUN wget http://www-eu.apache.org/dist/spark/spark-2.0.0-preview/spark-2.0.0-preview-bin-hadoop2.7.tgz
RUN gunzip -c spark-2.0.0-preview-bin-hadoop2.7.tgz |  tar xvf -
RUN HOME=/opt mv spark-2.0.0-preview-bin-hadoop2.7 /$HOME/spark 
RUN echo "done getting spark-submit"
RUN ls /$HOME/spark/bin
ADD . /opt/spark-perf/

WORKDIR /opt/spark-perf/
RUN chmod -R 777 /opt/spark-perf/*
# A quick run: This will bootstrap things as necessary so the images doesnt need to download SBT.
ADD driver-script.sh /opt/driver-script.sh

# Necessary if sbt bootstraps itself.
RUN mkdir /opt/.sbt && chmod -R 777 /opt/.sbt

#### Bootstrapping hacks.  not really sure whats required here, tried alot of things :)
RUN HOME=/opt /bin/sh -c SPARK_MASTER_URL=127.0.0.1:1234 /opt/driver-script.sh || echo "this is just for bootstrapping"
RUN HOME=/opt ./bin/run || echo "this is just for bootstrapping"

RUN HOME=/opt && chmod -R 777 ~/.sbt && chmod -R 777 ~/.ivy2 && chmod -R 777 /opt/spark-perf/*
RUN HOME=/opt && ls ~/.sbt && ls ~/.ivy2

RUN ls -altrh /opt/0.13.6/sbt-launch.jar

# Example CMD, most likely folks will override.
CMD HOME=/opt /opt/driver-script.sh
