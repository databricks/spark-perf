# Spark Performance Tests

This is a framework for repeatedly running a suite of performance tests for the [Spark cluster computing framework](http://spark-project.org).

The script assumes you already have a binary distribution of Spark 1.0+ installed. It can optionally checkout a new version of Spark and copy configurations over from your existing installation.

## Running locally

1. Download a `spark` 1.0+ binary distribution.
2. Set up local SSH server/keys such that `ssh localhost` works on your machine without a password.
3. Git clone spark-perf (this repo) and cd spark-perf
4. Copy config/config.py.template to config/config.py
5. Set config.py options that are friendly for local execution:
 * SPARK_HOME_DIR = /path/to/your/spark
 * SPARK_CLUSTER_URL = "spark://%s:7077" % socket.gethostname() 
 * SCALE_FACTOR = .05
 * spark.executor.memory = 2g
 * SPARK_DRIVER_MEMORY = 
 * uncomment at least one SPARK_TESTS entry
6. Execute bin/run

## Running on an existing Spark cluster
1. SSH into the machine hosting the standalone master
2. Git clone spark-perf (this repo) and cd spark-perf
3. Copy config/config.py.template to config/config.py
4. Set config.py options:
 * SPARK_HOME_DIR = /path/to/your/spark/install
 * SPARK_CLUSTER_URL = "spark://<your-master-hostname>:7077"
 * SCALE_FACTOR = <depends on your hardware>
 * SPARK_DRIVER_MEMORY = 
 * spark.executor.memory = <depends on your hardware>
 * uncomment at least one SPARK_TESTS entry
5. Execute bin/run

## Running on a spark-ec2 cluster with a custom Spark version
1. Launch an EC2 cluster with [spark-ec2 scripts](https://github.com/mesos/spark-ec2).
2. Git clone spark-perf (this repo) and cd spark-perf
3. Copy config/config.py.template to config/config.py
4. Set config.py options:
 * USE_CLUSTER_SPARK = False
 * SPARK_COMMIT_ID = <what you want test>
 * SCALE_FACTOR = <depends on your hardware>
 * SPARK_DRIVER_MEMORY = 
 * spark.executor.memory = <depends on your hardware>
 * uncomment at least one SPARK_TESTS entry
5. Execute bin/run

## Requirements
The script requires Python 2.7. For earlier versions of Python, argparse might need to be installed, 
which can be done using easy_install argparse.

## Acknowledgements
Questions or comments, contact @pwendell or @andyk.

This testing framework started as a port + heavy modifiation of a predecessor
Spark performance testing framework written by Denny Britz called
[spark-perf](https://github.com/dennybritz/spark-perf).
