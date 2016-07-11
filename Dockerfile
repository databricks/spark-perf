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
RUN mkdir -p /root/spark-ec2/
RUN cd /opt/ && git clone https://github.com/paulp/sbt-extras.git  && chmod 777 sbt-extras/* 
RUN ls -altrh /opt/sbt-extras/

RUN wget https://dl.bintray.com/sbt/native-packages/sbt/0.13.11/sbt-0.13.11.zip -O /opt/sbt.zip
RUN cd /opt/ && unzip sbt.zip
RUN ls -alrth /opt/sbt/bin/sbt-launch.jar
RUN dnf install -y which

ADD . /opt/spark-perf/
WORKDIR /opt/spark-perf/

CMD if [ -n "$SPARK_MASTER_URL" ]; then echo "FAILED! Missing spark master url" && exit 1 ; fi ; export PATH=$PATH:/opt/sbt-extras/ && echo $SPARK_MASTER_URL > /root/spark-ec2/cluster-url && ./bin/run
