#!/usr/bin/env python

import argparse
import imp
import re
import time

from sparkperf.commands import *
from sparkperf.cluster import Cluster
from sparkperf.testsuites import *


OUTPUT_DIVIDER_STRING = "-" * 68

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
should_prep_spark = (not config.SPARK_SKIP_PREP) and (not config.USE_CLUSTER_SPARK)

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

if config.USE_CLUSTER_SPARK:
  cluster = Cluster(spark_home=config.SPARK_HOME_DIR, spark_conf_dir=config.SPARK_CONF_DIR)
else:
    cluster = Cluster(spark_home="%s/spark" % PROJ_DIR, spark_conf_dir=config.SPARK_CONF_DIR)

# If a cluster is already running from an earlier test, try shutting it down.
if os.path.exists(cluster.spark_home):
    cluster.stop()

# Ensure all shutdowns have completed (no executors are running).
cluster.ensure_spark_stopped_on_slaves()
# Allow some extra time for slaves to fully terminate.
time.sleep(5)

# Prepare Spark.
if should_prep_spark:
    # Assumes that the preexisting 'spark' directory is valid.
    if not os.path.isdir("spark"):
        # Clone Spark.
        print("Git cloning Spark...")
        run_cmd("git clone %s spark" % config.SPARK_GIT_REPO)
        run_cmd("cd spark; git config --add remote.origin.fetch "
            "'+refs/pull/*/head:refs/remotes/origin/pr/*'")
        run_cmd("cd spark; git config --add remote.origin.fetch "
            "'+refs/tags/*:refs/remotes/origin/tag/*'")

    # Fetch updates.
    os.chdir("spark")
    print("Updating Spark repo...")
    run_cmd("git fetch")

    # Build Spark.
    print("Cleaning Spark and building branch %s. This may take a while...\n" %
        config.SPARK_COMMIT_ID)
    run_cmd("git clean -f -d -x")

    if config.SPARK_MERGE_COMMIT_INTO_MASTER:
        run_cmd("git reset --hard master")
        run_cmd("git merge %s -m ='Merging %s into master.'" %
            (config.SPARK_COMMIT_ID, config.SPARK_COMMIT_ID))
    else:
        run_cmd("git reset --hard %s" % config.SPARK_COMMIT_ID)

    run_cmd("%s clean assembly/assembly" % SBT_CMD)

    # Copy Spark configuration files to new directory.
    print("Copying all files from %s to %s/spark/conf/" % (config.SPARK_CONF_DIR, PROJ_DIR))
    assert os.path.exists("%s/spark-env.sh" % config.SPARK_CONF_DIR), \
        "Could not find required file %s/spark-env.sh" % config.SPARK_CONF_DIR
    assert os.path.exists("%s/slaves" % config.SPARK_CONF_DIR), \
        "Could not find required file %s/slaves" % config.SPARK_CONF_DIR
    run_cmd("cp %s/* %s/spark/conf/" % (config.SPARK_CONF_DIR, PROJ_DIR))

    # Change back to 'PROJ_DIR' directory.
    os.chdir("..")

    # Sync the whole directory to the slaves.
    print("Syncing Spark directory to the slaves.")

    make_PROJ_DIR = [(make_ssh_cmd("mkdir -p %s" % PROJ_DIR , s), True) for s in cluster.slaves]
    run_cmds_parallel(make_PROJ_DIR)
 
    copy_spark = [(make_rsync_cmd("%s/spark" % PROJ_DIR , s), True) for s in cluster.slaves]
    run_cmds_parallel(copy_spark)

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
    # Search for 'spark.local.dir' in spark-env.sh.
    spark_local_dirs = ""
    path_to_env_file = "%s/spark-env.sh" % config.SPARK_CONF_DIR
    env_file_content = open(path_to_env_file, 'r').read()
    re_result = re.search(r'SPARK_LOCAL_DIRS=(.*)', env_file_content)
    if re_result:
        spark_local_dirs = re_result.group(1).split(",")
    else:
        sys.exit("ERROR: These scripts require you to explicitly set SPARK_LOCAL_DIRS "
        "in spark-env.sh so that it can be cleaned. The way we check this is pretty picky, "
        "specifically we try to find the following string in spark-env.sh: "
        "SPARK_LOCAL_DIRS=ONE_OR_MORE_DIRNAMES\" so you will want a line like this: ")

    for local_dir in spark_local_dirs:

        # Strip off any trailing whitespace(s) so that the clear commands below can work properly.
        local_dir = local_dir.rstrip()

        bytes_to_write = config.DISK_WARMUP_BYTES
        bytes_per_file = bytes_to_write / config.DISK_WARMUP_FILES
        gen_command = "dd if=/dev/urandom bs=%s count=1 | split -a 5 -b %s - %s/random" % (
            bytes_to_write, bytes_per_file, local_dir)
        # Ensures the directory exists.
        dir_command = "mkdir -p %s" % local_dir
        clear_command = "rm -f %s/*" % local_dir

        print("Generating test data for %s, this may take some time" % local_dir)
        all_hosts = cluster.slaves + ["localhost"]
        run_cmds_parallel([(make_ssh_cmd(dir_command, host), True) for host in all_hosts])
        run_cmds_parallel([(make_ssh_cmd(gen_command, host), True) for host in all_hosts])
        clear_dir(local_dir, all_hosts, config.PROMPT_FOR_DELETES)

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
cluster.stop()

print("Finished running all tests.")
