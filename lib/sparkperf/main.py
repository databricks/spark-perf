#!/usr/bin/env python

import argparse
import imp
import time
import logging

logger = logging.getLogger("sparkperf")
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

from sparkperf.commands import *
from sparkperf.cluster import Cluster
from sparkperf.testsuites import *
from sparkperf.build import SparkBuildManager


parser = argparse.ArgumentParser(description='Run Spark or Shark peformance tests. Before running, '
    'edit the supplied configuration file.')

parser.add_argument('--config-file', help='override default location of config file, must be a '
    'python file that ends in .py', default="%s/config/config.py" % PROJ_DIR)

args = parser.parse_args()
assert args.config_file.endswith(".py"), "config filename must end with .py"

# Check if the config file exists.
assert os.path.isfile(args.config_file), ("Please create a config file called %s (you probably "
    "just want to copy and then modify %s/config/config.py.template)" %
    (args.config_file, PROJ_DIR))

print "Detected project directory: %s" % PROJ_DIR
# Import the configuration settings from the config file.
print "Loading configuration from %s" % args.config_file
with open(args.config_file) as cf:
    config = imp.load_source("config", "", cf)

# Spark will always be built, assuming that any possible test run of this program is going to depend
# on Spark.
has_spark_tests = not config.SPARK_SKIP_TESTS and (len(config.SPARK_TESTS) > 0)
should_prep_spark = not config.USE_CLUSTER_SPARK

# Since Spark is always going to prepared, streaming and mllib do not require extra preparation
has_streaming_tests = not config.STREAMING_SKIP_TESTS and (len(config.STREAMING_TESTS) > 0)
has_mllib_tests = not config.MLLIB_SKIP_TESTS and (len(config.MLLIB_TESTS) > 0)

# Only build the perf test sources that will be used.
should_prep_spark_tests = has_spark_tests and not config.SPARK_SKIP_TEST_PREP
should_prep_streaming_tests = has_streaming_tests and not config.STREAMING_SKIP_TEST_PREP
should_prep_mllib_tests = has_mllib_tests and not config.MLLIB_SKIP_TEST_PREP

# Do disk warmup only if there are tests to run.
should_warmup_disk = has_spark_tests and not config.SKIP_DISK_WARMUP

# Check that commit ID's are specified in config_file.
if should_prep_spark:
    assert config.SPARK_COMMIT_ID is not "", \
        ("Please specify SPARK_COMMIT_ID in %s" % args.config_file)

# If a cluster is already running from the Spark EC2 scripts, try shutting it down.
if os.path.exists(config.SPARK_HOME_DIR):
    Cluster(spark_home=config.SPARK_HOME_DIR).stop()

spark_build_manager = SparkBuildManager("%s/spark-build-cache" % PROJ_DIR, config.SPARK_GIT_REPO)

if config.USE_CLUSTER_SPARK:
    cluster = Cluster(spark_home=config.SPARK_HOME_DIR, spark_conf_dir=config.SPARK_CONF_DIR)
else:
    cluster = spark_build_manager.get_cluster(config.SPARK_COMMIT_ID, config.SPARK_CONF_DIR,
                                              config.SPARK_MERGE_COMMIT_INTO_MASTER)
    cluster.sync_spark()

# If a cluster is already running from an earlier test, try shutting it down.
if os.path.exists(cluster.spark_home):
    cluster.stop()

# Ensure all shutdowns have completed (no executors are running).
cluster.ensure_spark_stopped_on_slaves()
# Allow some extra time for slaves to fully terminate.
time.sleep(5)

# Build the tests for each project.
spark_work_dir = "%s/work" % cluster.spark_home
if os.path.exists(spark_work_dir):
    # Clear the 'perf-tests/spark/work' directory beforehand, since that may contain scheduler logs
    # from previous jobs. The directory could also contain a spark-perf-tests-assembly.jar that may
    # interfere with subsequent 'sbt assembly' for Spark perf.
    clear_dir(spark_work_dir, ["localhost"], config.PROMPT_FOR_DELETES)

print("Building perf tests...")
if should_prep_spark_tests:
    SparkTests.build()
elif has_spark_tests:
    assert SparkTests.is_built(), ("You tried to skip packaging the Spark perf " +
        "tests, but %s was not already present") % SparkTests.test_jar_path

if should_prep_streaming_tests:
    StreamingTests.build()
elif has_streaming_tests:
    assert StreamingTests.is_built(), ("You tried to skip packaging the Spark Streaming perf " +
        "tests, but %s was not already present") % StreamingTests.test_jar_path

if should_prep_mllib_tests:
    MLlibTests.build()
elif has_mllib_tests:
    assert MLlibTests.is_built(), ("You tried to skip packaging the MLlib perf " +
        "tests, but %s was not already present") % MLlibTests.test_jar_path

# Start our Spark cluster.
print("Starting a Spark standalone cluster to use for testing...")
cluster.start()
time.sleep(5) # Starting the cluster takes a little time so give it a second.

if should_warmup_disk:
    cluster.warmup_disks(config.DISK_WARMUP_BYTES, config.DISK_WARMUP_FILES)

if has_spark_tests:
    SparkTests.run_tests(cluster, config, config.SPARK_TESTS, "Spark-Tests",
                         config.SPARK_OUTPUT_FILENAME)

if config.PYSPARK_TESTS:
    PythonTests.run_tests(cluster, config, config.PYSPARK_TESTS, "PySpark-Tests",
                          config.PYSPARK_OUTPUT_FILENAME)

if has_streaming_tests:
    StreamingTests.run_tests(cluster, config, config.STREAMING_TESTS, "Streaming-Tests",
                             config.STREAMING_OUTPUT_FILENAME)

if has_mllib_tests:
    MLlibTests.run_tests(cluster, config, config.MLLIB_TESTS, "MLlib-Tests",
                         config.MLLIB_OUTPUT_FILENAME)

print("All tests have finished running. Stopping Spark standalone cluster ...")
# cluster.stop()

print("Finished running all tests.")
