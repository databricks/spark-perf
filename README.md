# Spark Performance Tests

This is a framework for repeatedly running a suite of performance tests for the [Spark cluster computing framework](http://spark-project.org).

## Running the Tests
1. Start a spark cluster for the tests using the [Spark EC2 scripts](http://spark-project.org/docs/latest/ec2-scripts.html)
2. SSH into the Spark master and git clone spark-perf
3. cd spark-perf
4. copy config/config.py.template to config/config.py and modify as necessary. Specifically, you must set COMMIT_ID.
5. execute bin/run

## Developing and Running on a Non-EC2 Cluster
The default configuration settings aim to make it easy to run on Amazon using the Spark EC2
scripts. To run in another environment, customize config.py. For example, when developing and
testing this framework, we recommend running a master and slave daemon on your development machine
(the test framework will start and stop this cluster for you with the correct config settings).
This exercises production code paths and avoids the need for extra code to support testing
locally. See DEVELOPER-NOTES.txt for a list of the variables you probably want to update and
possible suggestions for values to use.

## Requirements
The script requires Python 2.7. For earlier versions of Python, argparse might need to be installed, 
which can be done using easy_install argparse.

## Acknowledgements
Questions or comments, contact @pwendell or @andyk.

This testing framework started as a port + heavy modifiation of a predecessor
Spark performance testing framework written by Denny Britz called
[spark-perf](https://github.com/dennybritz/spark-perf).
