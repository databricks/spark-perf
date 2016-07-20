#!/bin/bash

# This is  a script which a user would edit when attempting to use docker to run the spark tests.
# Example usage...
# 1) Copy/modify this and then save it as "myrun.sh"
# 2) docker -v myrun.sh:/opt/driver-script.sh -t -i run xyz/spark-perf 

if [ ! -n "$SPARK_MASTER_URL" ]; then 
	echo "`env` .... FAILED! Missing spark master url" ; exit 1 ; 
else
	echo "spark master url $SPARK_MASTER_URL"
fi 
export PATH=$PATH:/opt/spark-perf/spark-tests/sbt/ 
echo "spark://$SPARK_MASTER_URL" > /root/spark-ec2/cluster-url

echo "cluster url ============"
cat /root/spark-ec2/cluster-url

echo "dir = `pwd`"


echo " ******* START !!!!!!!"

HOME=/opt/ ./bin/run
