#!/usr/bin/env python

import argparse
import itertools
import imp
from math import sqrt
import os
import os.path
import re
from subprocess import Popen, PIPE
import sys
import threading
import time

from sparkperf import PROJ_DIR

# The perf-tests project directory.
sbt_cmd = "sbt/sbt"

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
has_spark_tests = (len(config.SPARK_TESTS) > 0)
should_prep_spark = (not config.SPARK_SKIP_PREP) and (not config.USE_CLUSTER_SPARK)

# Since Spark is always going to prepared, streaming and mllib do not require extra preparation
has_streaming_tests = (len(config.STREAMING_TESTS) > 0)
has_mllib_tests = (len(config.MLLIB_TESTS) > 0)
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

# Run shell command and ignore output.
def run_cmd(cmd, exit_on_fail=True):
    if cmd.find(";") != -1:
        print("***************************")
        print("WARNING: the following command contains a semicolon which may cause non-zero return "
            "values to be ignored. This isn't necessarily a problem, but proceed with caution!")
    print(cmd)
    return_code = Popen(cmd, stdout=sys.stderr, shell=True).wait()
    if exit_on_fail:
        if return_code != 0:
            print "The following shell command finished with a non-zero returncode (%s): %s" % (
                return_code, cmd)
            sys.exit(-1)
    return return_code

# Run several commands in parallel, waiting for them all to finish.
# Expects an array of tuples, where each tuple consists of (command_name, exit_on_fail).
def run_cmds_parallel(commands):
    threads = []
    for (cmd_name, exit_on_fail) in commands:
        thread = threading.Thread(target=run_cmd, args=(cmd_name, exit_on_fail))
        thread.start()
        threads = threads + [thread]
    for thread in threads:
        thread.join()

# Return a command running cmd_name on host with proper SSH configs.
def make_ssh_cmd(cmd_name, host):
    return "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 %s '%s'" % (host, cmd_name)

# Return a command which copies the supplied directory to the given host.
def make_rsync_cmd(dir_name, host):
    return ('rsync --delete -e "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5" -az "%s/" '
        '"%s:%s"') % (dir_name, host, os.path.abspath(dir_name))

# Delete all files in the given directory on the specified hosts.
def clear_dir(dir_name, hosts):
    assert dir_name != "" and dir_name != "/", ("Attempted to delete directory '%s/*', halting "
        "rather than deleting entire file system.") % dir_name
    if config.PROMPT_FOR_DELETES:
        response = raw_input("\nAbout to remove all files and directories under %s on %s, is "
            "this ok? [y, n] " % (dir_name, hosts))
        if response != "y":
            return
    run_cmds_parallel([(make_ssh_cmd("rm -r %s/*" % dir_name, host), False) for host in hosts])

# Ensures that no executors are running on Spark slaves. Executors can continue to run for some
# time after a shutdown signal is given due to cleaning up temporary files.
def ensure_spark_stopped_on_slaves(slaves):
    stop = False
    while not stop:
        cmd = "ps -ef | grep -v grep | grep ExecutorBackend"
        ret_vals = map(lambda s: run_cmd(make_ssh_cmd(cmd, s), False), slaves)
        if 0 in ret_vals:
            print "Spark is still running on some slaves ... sleeping for 10 seconds"
            time.sleep(10)
        else:
            stop = True

# Get a list of slaves by parsing the slaves file in SPARK_CONF_DIR.
slaves_file_raw = open("%s/slaves" % config.SPARK_CONF_DIR, 'r').read().split("\n")
slaves_list = filter(lambda x: not x.startswith("#") and not x is "", slaves_file_raw)

# If a cluster is already running from the Spark EC2 scripts, try shutting it down.
if os.path.exists("%s/sbin/stop-all.sh" % config.SPARK_HOME_DIR):
    run_cmd("%s/sbin/stop-all.sh" % config.SPARK_HOME_DIR)

effective_spark_home = "%s/spark" % PROJ_DIR
if config.USE_CLUSTER_SPARK:
  effective_spark_home = config.SPARK_HOME_DIR

# If a cluster is already running from an earlier test, try shutting it down.
if os.path.exists("%s/sbin/stop-all.sh" % effective_spark_home):
    print("Stopping Spark standalone cluster...")
    run_cmd("%s/sbin/stop-all.sh" % effective_spark_home)

# Ensure all shutdowns have completed (no executors are running).
ensure_spark_stopped_on_slaves(slaves_list)
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

    run_cmd("%s clean assembly/assembly" % sbt_cmd)

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

    make_PROJ_DIR = [(make_ssh_cmd("mkdir -p %s" % PROJ_DIR , s), True) for s in slaves_list]
    run_cmds_parallel(make_PROJ_DIR)
 
    copy_spark = [(make_rsync_cmd("%s/spark" % PROJ_DIR , s), True) for s in slaves_list]
    run_cmds_parallel(copy_spark)

# Build the tests for each project.
spark_work_dir = "%s/work" % effective_spark_home
if os.path.exists(spark_work_dir):
    # Clear the 'perf-tests/spark/work' directory beforehand, since that may contain scheduler logs
    # from previous jobs. The directory could also contain a spark-perf-tests-assembly.jar that may
    # interfere with subsequent 'sbt assembly' for Spark perf.
    clear_dir(spark_work_dir, ["localhost"])

print("Building perf tests...")
if should_prep_spark_tests:
    run_cmd("cd %s/spark-tests; %s clean assembly" % (PROJ_DIR, sbt_cmd))
elif has_spark_tests:
    spark_test_jar_path = "%s/spark-tests/target/spark-perf-tests-assembly.jar" % PROJ_DIR
    assert os.path.exists(spark_test_jar_path), ("You tried to skip packaging the Spark perf " +
        "tests, but %s was not already present") % spark_test_jar_path

if should_prep_streaming_tests:
    run_cmd("cd %s/streaming-tests; %s clean assembly" % (PROJ_DIR, sbt_cmd))
elif has_streaming_tests:
    spark_test_jar_path = "%s/streaming-tests/target/streaming-perf-tests-assembly.jar" % PROJ_DIR
    assert os.path.exists(spark_test_jar_path), ("You tried to skip packaging the Spark Streaming perf " +
        "tests, but %s was not already present") % spark_test_jar_path

if should_prep_mllib_tests:
    run_cmd("cd %s/mllib-tests; %s clean assembly" % (PROJ_DIR, sbt_cmd))
elif has_mllib_tests:
    spark_test_jar_path = "%s/mllib-tests/target/mllib-perf-tests-assembly.jar" % PROJ_DIR
    assert os.path.exists(spark_test_jar_path), ("You tried to skip packaging the MLlib perf " +
        "tests, but %s was not already present") % spark_test_jar_path

# Start our Spark cluster.
print("Starting a Spark standalone cluster to use for testing...")
run_cmd("%s/sbin/start-all.sh" % effective_spark_home)
time.sleep(5) # Starting the cluster takes a little time so give it a second.

# Set Spark Java Options (from config.py)
test_env = os.environ.copy()

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
        all_hosts = slaves_list + ["localhost"]
        run_cmds_parallel([(make_ssh_cmd(dir_command, host), True) for host in all_hosts])
        run_cmds_parallel([(make_ssh_cmd(gen_command, host), True) for host in all_hosts])
        clear_dir(local_dir, all_hosts)

# Some utility functions to calculate useful stats on test output.
def average(in_list):
    return sum(in_list) / len(in_list)

def variance(in_list):
    variance = 0
    for x in in_list:
        variance = variance + (average(in_list) - x) ** 2
    return variance / len(in_list)

# Append configuration of a test run to a log file
def append_config_to_file(filename, java_opt_list, opt_list):
    output_divider_string = "--------------------------------------------------------------------"
    with open (filename, "a") as fout:
        fout.write(output_divider_string + "\n")
        fout.write("Java options: %s\n" % (" ".join(java_opt_list)))
        fout.write("Options: %s\n" % (" ".join(opt_list)))
        fout.write(output_divider_string + "\n")
        fout.flush()
    

# Run all tests specified in 'tests_to_run', a list of 5-element tuples. See the 'Test Setup'
# section in config.py.template for more info.
# Results are written to 'output_filename'.
#
# TODO: (written by TD) 
# This is an intermediate solution that takes the function to process the output of the tests as a function.
# This is done because Spark/Shark tests and Streaming tests process the output differently. Ideal solution, is
# that each test will do whatever processing necessary for it (e.g. generating stats on multiple trials) and return a 
# single string as a result that will written to the output file. 

def run_tests(test_jar, tests_to_run, test_group_name, output_processing_function, output_filename):
    output_dirname = output_filename + "_logs"
    os.makedirs(output_dirname)
    out_file = open(output_filename, 'w')
    num_tests_to_run = len(tests_to_run)

    output_divider_string = "--------------------------------------------------------------------"
    print(output_divider_string)
    print("Running %d tests in %s.\n" % (num_tests_to_run, test_group_name))
    failed_tests = []
    for short_name, main_class, scale_factor, java_opt_sets, opt_sets in tests_to_run:
        print(output_divider_string)
        print("Running test command: '%s' ..." % main_class)
        stdout_filename = "%s/%s.out" % (output_dirname, short_name) 
        stderr_filename = "%s/%s.err" % (output_dirname, short_name) 

        # Run a test for all combinations of the OptionSets given, then capture
        # and print the output.
        java_opt_set_arrays = [i.to_array(scale_factor) for i in java_opt_sets]
        opt_set_arrays = [i.to_array(scale_factor) for i in opt_sets]
        for java_opt_list in itertools.product(*java_opt_set_arrays):
            for opt_list in itertools.product(*opt_set_arrays):
                ensure_spark_stopped_on_slaves(slaves_list)
                append_config_to_file(stdout_filename, java_opt_list, opt_list)
                append_config_to_file(stderr_filename, java_opt_list, opt_list)
                test_env["SPARK_SUBMIT_OPTS"] = " ".join(java_opt_list)
                spark_submit = "%s/bin/spark-submit" % effective_spark_home
                cmd = "%s --class %s --master %s --driver-memory %s %s %s 1>> %s 2>> %s" % (
                    spark_submit, main_class, config.SPARK_CLUSTER_URL, config.SPARK_DRIVER_MEMORY,
                    test_jar, " ".join(opt_list), stdout_filename, stderr_filename)

                print("\nRunning command: %s\n" % cmd)
                Popen(cmd, shell=True, env=test_env).wait()
                result_string = output_processing_function(short_name, opt_list, stdout_filename, stderr_filename)
                print(output_divider_string)
                print("\nResult: " + result_string) 
                print(output_divider_string)
                if "FAILED" in result_string:
                    failed_tests.append(short_name)
                out_file.write(result_string + "\n")
                out_file.flush()

    print("\nFinished running %d tests in %s.\nSee summary in %s" %
        (num_tests_to_run, test_group_name, output_filename))
    print("\nNumber of failed tests: %d, failed tests: %s" %
        (len(failed_tests), ",".join(failed_tests)))
    print(output_divider_string)

def stats_for_results(result_list):
    result_first = result_list[0]
    result_last = result_list[-1]
    sorted_results = sorted([float(x) for x in result_list])
    result_med = sorted_results[len(sorted_results)/2]
    if (len(result_list) % 2 == 0):
        result_med = (sorted_results[len(result_list)/2] + sorted_results[len(result_list)/2+1])/2
    result_std = sqrt(variance(sorted_results))
    result_min = sorted_results[0]

    return (result_med, result_std, result_min, result_first, result_last)

def run_python_tests(tests, test_group_name, output_processing_function, output_filename):
    output_dirname = output_filename + "_logs"
    os.makedirs(output_dirname)
    out_file = open(output_filename, 'w')
    num_tests_to_run = len(tests)

    output_divider_string = "--------------------------------------------------------------------"
    print(output_divider_string)
    print("Running %d tests in %s.\n" % (num_tests_to_run, test_group_name))

    for short_name, scripts, scale_factor, java_opt_sets, opt_sets in tests:
        print(output_divider_string)
        print("Running test command: '%s' ..." % scripts)
        stdout_filename = "%s/%s.out" % (output_dirname, short_name)
        stderr_filename = "%s/%s.err" % (output_dirname, short_name)

        # Run a test for all combinations of the OptionSets given, then capture
        # and print the output.
        java_opt_set_arrays = [i.to_array(scale_factor) for i in java_opt_sets]
        opt_set_arrays = [i.to_array(scale_factor) for i in opt_sets]
        for java_opt_list in itertools.product(*java_opt_set_arrays):
            for opt_list in itertools.product(*opt_set_arrays):
                ensure_spark_stopped_on_slaves(slaves_list)
                append_config_to_file(stdout_filename, java_opt_list, opt_list)
                append_config_to_file(stderr_filename, java_opt_list, opt_list)
                test_env["SPARK_SUBMIT_OPTS"] = " ".join(java_opt_list)
                spark_submit = "%s/bin/spark-submit" % effective_spark_home
                cmd = "%s --master %s pyspark-tests/%s %s 1>> %s 2>> %s" % (
                    spark_submit, config.SPARK_CLUSTER_URL, scripts, " ".join(opt_list),
                    stdout_filename, stderr_filename)

                print("\nRunning command: %s\n" % cmd)
                Popen(cmd, shell=True, env=test_env).wait()
                result_string = output_processing_function(short_name, opt_list, stdout_filename, stderr_filename)
                print(output_divider_string)
                print("\nResult: " + result_string)
                print(output_divider_string)
                out_file.write(result_string + "\n")
                out_file.flush()

    print("\nFinished running %d tests in %s.\nSee summary in %s" %
          (num_tests_to_run, test_group_name, output_filename))
    print(output_divider_string)

# Function to process the output of multiple trials for Spark and Shark tests 
def process_trials_output(short_name, opt_list, stdout_filename, stderr_filename):
    with open (stdout_filename, "r") as stdout_file:
        output = stdout_file.read()
    results_token = "results: "
    if results_token not in output:
        print("Test did not produce expected results. Output was:")
        print(output)
        sys.exit(1)
    result_line = filter(lambda x: results_token in x, output.split("\n"))[-1]
    result_list = result_line.replace(results_token, "").split(",")
    assert len(result_list) > config.IGNORED_TRIALS, ("Expecting at least %s results "
        "but only found %s" % (config.IGNORED_TRIALS + 1, len(result_list)))
    result_list = result_list[config.IGNORED_TRIALS:]

    result_string = "%s, %s, " % (short_name, " ".join(opt_list))

    result_string += "%s, %.3f, %s, %s, %s\n" % stats_for_results(result_list)

    sys.stdout.flush()
    return result_string

# Function to process output of streaming tests
def process_streaming_output(short_name, opt_list, stdout_filename, stderr_filename):
    lastlines = Popen("tail -5 %s" % stdout_filename, shell=True, stdout=PIPE).stdout.read()
    # result_token = "PASSED"
    results_token = "Result: "
    if results_token not in lastlines:
        result = "FAILED"
    else:
        result = filter(lambda x: results_token in x, lastlines.split("\n"))[0].replace(results_token, "")
    result_string = "%s [ %s ] - %s" % (short_name, " ".join(opt_list), result)
    return str(result_string)

# Function to process the output of multiple trials for MLlib tests 
def process_mllib_output(short_name, opt_list, stdout_filename, stderr_filename):
    with open (stdout_filename, "r") as stdout_file:
        output = stdout_file.read()
    results_token = "results: "
    result = ""
    if results_token not in output:
        result = "FAILED"
    else:
        result_line = filter(lambda x: results_token in x, output.split("\n"))[-1]
        result_list = result_line.replace(results_token, "").split(",")
        assert len(result_list) > config.IGNORED_TRIALS, ("Expecting at least %s results "
            "but only found %s" % (config.IGNORED_TRIALS + 1, len(result_list)))
        result_list = result_list[config.IGNORED_TRIALS:]
        result += "Runtime: %s, %.3f, %s, %s, %s\n" % stats_for_results([x.split(";")[0] for x in result_list])
        result += "Train Set Metric: %s, %.3f, %s, %s, %s\n" % stats_for_results([x.split(";")[1] for x in result_list])
        result += "Test Set Metric: %s, %.3f, %s, %s, %s" % stats_for_results([x.split(";")[2] for x in result_list])

    result_string = "%s, %s\n%s" % (short_name, " ".join(opt_list), result)

    sys.stdout.flush()
    return result_string

if has_spark_tests:
    spark_test_jar = "%s/spark-tests/target/spark-perf-tests-assembly.jar" % PROJ_DIR
    run_tests(spark_test_jar, config.SPARK_TESTS, "Spark-Tests",
        process_trials_output, config.SPARK_OUTPUT_FILENAME)

if config.PYSPARK_TESTS:
    run_python_tests(config.PYSPARK_TESTS, "PySpark-Tests",
                     process_trials_output, config.PYSPARK_OUTPUT_FILENAME)

if has_streaming_tests:
    streaming_test_jar = "%s/streaming-tests/target/streaming-perf-tests-assembly.jar" % PROJ_DIR
    run_tests(streaming_test_jar, config.STREAMING_TESTS, "Streaming-Tests",
        process_streaming_output, config.STREAMING_OUTPUT_FILENAME)

if has_mllib_tests:
    mllib_test_jar = "%s/mllib-tests/target/mllib-perf-tests-assembly.jar" % PROJ_DIR
    run_tests(mllib_test_jar, config.MLLIB_TESTS, "MLlib-Tests",
        process_mllib_output, config.MLLIB_OUTPUT_FILENAME)

print("All tests have finished running. Stopping Spark standalone cluster ...")
run_cmd("%s/sbin/stop-all.sh" % effective_spark_home)

print("Finished running all tests.")
