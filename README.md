# Spark Performance Tests

[![Build Status](https://travis-ci.org/databricks/spark-perf.svg?branch=master)](https://travis-ci.org/databricks/spark-perf)

This is a performance testing framework for [Apache Spark](http://spark.apache.org) 1.0+.

**Features**:

- Suites of performance tests for Spark, PySpark, Spark Streaming, and MLlib.
- Parameterized test configurations:
   - Sweeps sets of parameters to test against multiple Spark and test configurations.
- Automatically downloads and builds Spark:
   - Maintains a cache of successful builds to enable rapid testing against multiple Spark versions.
- [...]

For questions, bug reports, or feature requests, please [open an issue on GitHub](https://github.com/databricks/spark-perf/issues).

## Requirements

The `spark-perf` scripts require Python 2.7+.  If you're using an earlier version of Python, you may need to install the `argparse` library using `easy_install argparse`.

Support for automatically building Spark requires Maven.  On `spark-ec2` clusters, this can be installed using the `./bin/spark-ec2/install-maven` script from this project.


## Configuration

To configure `spark-perf`, copy `config/config.py.template` to `config/config.py` and edit that file.  See `config.py.template` for detailed configuration instructions.  After editing `config.py`, execute `./bin/run` to run performance tests.  You can pass the `--config` option to use a custom configuration file.

The following sections describe some additional settings to change for certain test environments:

### Running locally

1. Set up local SSH server/keys such that `ssh localhost` works on your machine without a password.
2. Set config.py options that are friendly for local execution:

   ```
   SPARK_HOME_DIR = /path/to/your/spark
   SPARK_CLUSTER_URL = "spark://%s:7077" % socket.gethostname()
   SCALE_FACTOR = .05
   SPARK_DRIVER_MEMORY = 512m
   spark.executor.memory = 2g
   ```
3. Uncomment at least one `SPARK_TESTS` entry

### Running on an existing Spark cluster
1. SSH into the machine hosting the standalone master
2. Set config.py options:

   ```
   SPARK_HOME_DIR = /path/to/your/spark/install
   SPARK_CLUSTER_URL = "spark://<your-master-hostname>:7077"
   SCALE_FACTOR = <depends on your hardware>
   SPARK_DRIVER_MEMORY = <depends on your hardware>
   spark.executor.memory = <depends on your hardware>
   ```
3. Uncomment at least one `SPARK_TESTS` entry

### Running on a spark-ec2 cluster with a custom Spark version
1. Launch an EC2 cluster with [Spark's EC2 scripts](https://spark.apache.org/docs/latest/ec2-scripts.html).
2. Set config.py options:

   ```
   USE_CLUSTER_SPARK = False
   SPARK_COMMIT_ID = <what you want test>
   SCALE_FACTOR = <depends on your hardware>
   SPARK_DRIVER_MEMORY = <depends on your hardware>
   spark.executor.memory = <depends on your hardware>
   ```
3. uncomment at least one `SPARK_TESTS` entry


## Acknowledgements

This testing framework started as a port + heavy modification of an earlier Spark performance testing framework written by @dennybritz.
