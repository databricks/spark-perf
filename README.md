# Spark Peformance Tests

This is a framework for repeatedly running a suite of performance tests for the [Spark cluster computing framework](http://spark-project.org).

## Running the Tests
1. Start a spark cluster for the tests using the [Spark EC2 scripts](http://spark-project.org/docs/latest/ec2-scripts.html)
2. SSH into the Spark master and git clone perf-tests
3. cd perfs-tests
4. copy config/config.py.template to config/config.py and modify as necessary (specifically, you must set COMMIT_ID, and you probabaly want to update SPARK_CLUSTER_URL and SCALA_HOME)
5. `bin/run`

## Acknowledgements
Questions or comments, contact @pwendell or @andyk.

This testing framework started as a port + heavy modifiation of a predecessor
Spark performance testing framework written by Denny Britz called
[spark-perf](https://github.com/dennybritz/spark-perf).
