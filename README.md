= Spark Peformance Tests =

This is a framework for repeatedly running a suite of performance tests for the [Spark cluster computing framework](http://spark-project.org).

== Requirements ==
* We currently assume that an instance of Spark is already running (or that you will use local mode, e.g. for the purpose of testing these scripts) and you will configure the tests to use it (see the instructions on running the tests below for more details about how).
* /root/spark-ec2/copy-dir is used to copy the perf jars from the Spark Master to all Spark slaves. This is based on the assumption that you are probably running the tests using the spark EC2 AMI.

== Running the Tests ==
# Start a spark cluster to run the tests on
# SSH into the Spark master and git clone perf-tests
# cd spark-perfs
# copy config/config.py.template to config/config.py and modify as necessary
# `bin/run [hash of spark commit to test] [output results csv filename]`

== Notes ==
* We use a slightly modified version (as of the time this was written) of the [spark-ec2/copy-dir script](https://github.com/mesos/spark-ec2/blob/bf8b4155a1fcd6fc5c1141323858fd6d021ce6a3/copy-dir.sh).

== Acknowledgements ==
This testing framework started as a port + heavy modifiation of a predecessor
Spark performance testing framework written by Denny Britz called
[spark-perf](https://github.com/dennybritz/spark-perf).
