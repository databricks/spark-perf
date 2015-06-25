import itertools
import os
from subprocess import Popen, PIPE
import sys
import json

from sparkperf import PROJ_DIR
from sparkperf.commands import run_cmd, SBT_CMD
from sparkperf.utils import OUTPUT_DIVIDER_STRING, append_config_to_file, stats_for_results


test_env = os.environ.copy()


class PerfTestSuite(object):

    @classmethod
    def build(cls):
        """
        Performs any compilation needed to prepare this test suite.
        """
        pass

    @classmethod
    def is_built(cls):
        """
        :return: True if this test suite has been built / compiled.
        """
        return True

    @classmethod
    def process_output(cls, config, short_name, opt_list, stdout_filename, stderr_filename):
        raise NotImplementedError

    @classmethod
    def before_run_tests(cls, config, out_file):
        """
        This is called before tests in this suite are run.
        It is useful for logging test suite specific messages.

        :param out_file: a python file handler to the output file
        """
        pass

    @classmethod
    def run_tests(cls, cluster, config, tests_to_run, test_group_name, output_filename):
        """
        Run a set of tests from this performance suite.

        :param cluster:  The L{Cluster} to run the tests on.
        :param tests_to_run:  A list of 5-tuple elements specifying the tests to run.  See the
                             'Test Setup' section in config.py.template for more info.
        :param test_group_name:  A short string identifier for this test run.
        :param output_filename:  The output file where we write results.
        """
        output_dirname = output_filename + "_logs"
        os.makedirs(output_dirname)
        out_file = open(output_filename, 'w')
        num_tests_to_run = len(tests_to_run)

        print(OUTPUT_DIVIDER_STRING)
        print("Running %d tests in %s.\n" % (num_tests_to_run, test_group_name))
        failed_tests = []

        cls.before_run_tests(config, out_file)

        for short_name, main_class_or_script, scale_factor, java_opt_sets, opt_sets in tests_to_run:
            print(OUTPUT_DIVIDER_STRING)
            print("Running test command: '%s' ..." % main_class_or_script)
            stdout_filename = "%s/%s.out" % (output_dirname, short_name)
            stderr_filename = "%s/%s.err" % (output_dirname, short_name)

            # Run a test for all combinations of the OptionSets given, then capture
            # and print the output.
            java_opt_set_arrays = [i.to_array(scale_factor) for i in java_opt_sets]
            opt_set_arrays = [i.to_array(scale_factor) for i in opt_sets]
            for java_opt_list in itertools.product(*java_opt_set_arrays):
                for opt_list in itertools.product(*opt_set_arrays):
                    cluster.ensure_spark_stopped_on_slaves()
                    append_config_to_file(stdout_filename, java_opt_list, opt_list)
                    append_config_to_file(stderr_filename, java_opt_list, opt_list)
                    java_opts_str = " ".join(java_opt_list)
                    java_opts_str += " -Dsparkperf.commitSHA=" + cluster.commit_sha
                    if hasattr(config, 'SPARK_EXECUTOR_URI'):
                        java_opts_str += " -Dspark.executor.uri=" + config.SPARK_EXECUTOR_URI
                    if hasattr(config, 'SPARK_MESOS_COARSE') and config.SPARK_MESOS_COARSE:
                        java_opts_str += " -Dspark.mesos.coarse=true"
                    cmd = cls.get_spark_submit_cmd(cluster, config, main_class_or_script, opt_list,
                                                   stdout_filename, stderr_filename)
                    print("\nSetting env var SPARK_SUBMIT_OPTS: %s" % java_opts_str)
                    test_env["SPARK_SUBMIT_OPTS"] = java_opts_str
                    if hasattr(config, 'MESOS_NATIVE_LIBRARY'):
                        print("\nSetting env var MESOS_NATIVE_LIBRARY: %s" % config.MESOS_NATIVE_LIBRARY)
                        test_env["MESOS_NATIVE_LIBRARY"] = config.MESOS_NATIVE_LIBRARY
                    print("Running command: %s\n" % cmd)
                    Popen(cmd, shell=True, env=test_env).wait()
                    result_string = cls.process_output(config, short_name, opt_list,
                                                       stdout_filename, stderr_filename)
                    print(OUTPUT_DIVIDER_STRING)
                    print("\nResult: " + result_string)
                    print(OUTPUT_DIVIDER_STRING)
                    if "FAILED" in result_string:
                        failed_tests.append(short_name)
                    out_file.write(result_string + "\n")
                    out_file.flush()

            print("\nFinished running %d tests in %s.\nSee summary in %s" %
                  (num_tests_to_run, test_group_name, output_filename))
            print("\nNumber of failed tests: %d, failed tests: %s" %
                  (len(failed_tests), ",".join(failed_tests)))
            print(OUTPUT_DIVIDER_STRING)

    @classmethod
    def get_spark_submit_cmd(cls, cluster, config, main_class_or_script, opt_list, stdout_filename,
                             stderr_filename):
        raise NotImplementedError


class JVMPerfTestSuite(PerfTestSuite):
    test_jar_path = "/path/to/test/jar"

    @classmethod
    def is_built(cls):
        return os.path.exists(cls.test_jar_path)

    @classmethod
    def get_spark_submit_cmd(cls, cluster, config, main_class_or_script, opt_list, stdout_filename,
                             stderr_filename):
        spark_submit = "%s/bin/spark-submit" % cluster.spark_home
        cmd = "%s --class %s --master %s --driver-memory %s %s %s 1>> %s 2>> %s" % (
            spark_submit, main_class_or_script, config.SPARK_CLUSTER_URL,
            config.SPARK_DRIVER_MEMORY, cls.test_jar_path, " ".join(opt_list),
            stdout_filename, stderr_filename)
        return cmd


class SparkTests(JVMPerfTestSuite):
    test_jar_path = "%s/spark-tests/target/spark-perf-tests-assembly.jar" % PROJ_DIR

    @classmethod
    def build(cls):
        run_cmd("cd %s/spark-tests; %s clean assembly" % (PROJ_DIR, SBT_CMD))

    @classmethod
    def before_run_tests(cls, config, out_file):
        out_file.write("# Test name, test options, median, std dev, min, first, last\n")
        out_file.flush()

    @classmethod
    def process_output(cls, config, short_name, opt_list, stdout_filename, stderr_filename):
        with open(stdout_filename, "r") as stdout_file:
            output = stdout_file.read()
        results_token = "results: "
        if results_token not in output:
            print("Test did not produce expected results. Output was:")
            print(output)
            sys.exit(1)
        result_line = filter(lambda x: results_token in x, output.split("\n"))[-1]
        result_json = result_line.replace(results_token, "")
        result_dict = json.loads(result_json)
        times = [r['time'] for r in result_dict['results']]
        err_msg = ("Expecting at least %s results "
                   "but only found %s" % (config.IGNORED_TRIALS + 1, len(times)))
        assert len(times) > config.IGNORED_TRIALS, err_msg
        times = times[config.IGNORED_TRIALS:]

        result_string = "%s, %s, " % (short_name, " ".join(opt_list))
        result_string += "%s, %.3f, %s, %s, %s\n" % stats_for_results(times)

        sys.stdout.flush()
        return result_string


class StreamingTests(JVMPerfTestSuite):
    test_jar_path = "%s/streaming-tests/target/streaming-perf-tests-assembly.jar" % PROJ_DIR

    @classmethod
    def build(cls):
        run_cmd("cd %s/streaming-tests; %s clean assembly" % (PROJ_DIR, SBT_CMD))

    @classmethod
    def process_output(cls, config, short_name, opt_list, stdout_filename, stderr_filename):
        lastlines = Popen("tail -5 %s" % stdout_filename, shell=True, stdout=PIPE).stdout.read()
        results_token = "Result: "
        if results_token not in lastlines:
            result = "FAILED"
        else:
            result = filter(lambda x: results_token in x,
                            lastlines.split("\n"))[0].replace(results_token, "")
        result_string = "%s [ %s ] - %s" % (short_name, " ".join(opt_list), result)
        return str(result_string)


class MLlibTestHelper(object):

    @classmethod
    def process_output(cls, config, short_name, opt_list, stdout_filename, stderr_filename):
        with open(stdout_filename, "r") as stdout_file:
            output = stdout_file.read()
        results_token = "results: "
        result_string = ""
        if results_token not in output:
            result_string = "FAILED"
        else:
            result_line = filter(lambda x: results_token in x, output.split("\n"))[-1]
            result_json = result_line.replace(results_token, "")
            try:
                result_dict = json.loads(result_json)
            except:
                print "Failed to parse JSON:\n", result_json
                raise

            num_results = len(result_dict['results'])
            err_msg = ("Expecting at least %s results "
                       "but only found %s" % (config.IGNORED_TRIALS + 1, num_results))
            assert num_results > config.IGNORED_TRIALS, err_msg

            # 2 modes: prediction problems (4 metrics) and others (time only)
            if 'trainingTime' in result_dict['results'][0]:
                # prediction problem
                trainingTimes = [r['trainingTime'] for r in result_dict['results']]
                testTimes = [r['testTime'] for r in result_dict['results']]
                trainingMetrics = [r['trainingMetric'] for r in result_dict['results']]
                testMetrics = [r['testMetric'] for r in result_dict['results']]
                trainingTimes = trainingTimes[config.IGNORED_TRIALS:]
                testTimes = testTimes[config.IGNORED_TRIALS:]
                trainingMetrics = trainingMetrics[config.IGNORED_TRIALS:]
                testMetrics = testMetrics[config.IGNORED_TRIALS:]
                result_string += "Training time: %s, %.3f, %s, %s, %s\n" % \
                                 stats_for_results(trainingTimes)
                result_string += "Test time: %s, %.3f, %s, %s, %s\n" % \
                                 stats_for_results(testTimes)
                result_string += "Training Set Metric: %s, %.3f, %s, %s, %s\n" % \
                                 stats_for_results(trainingMetrics)
                result_string += "Test Set Metric: %s, %.3f, %s, %s, %s" % \
                                 stats_for_results(testMetrics)
            else:
                # non-prediction problem
                times = [r['time'] for r in result_dict['results']]
                times = times[config.IGNORED_TRIALS:]
                result_string += "Time: %s, %.3f, %s, %s, %s\n" % \
                                 stats_for_results(times)

        result_string = "%s, %s\n%s" % (short_name, " ".join(opt_list), result_string)

        sys.stdout.flush()
        return result_string


class MLlibTests(JVMPerfTestSuite, MLlibTestHelper):
    test_jar_path = "%s/mllib-tests/target/mllib-perf-tests-assembly.jar" % PROJ_DIR

    @classmethod
    def build(cls, spark_version):
        run_cmd("cd %s/mllib-tests; %s -Dspark.version=%s clean assembly" % (PROJ_DIR, SBT_CMD, spark_version))

    @classmethod
    def process_output(cls, config, short_name, opt_list, stdout_filename, stderr_filename):
        return MLlibTestHelper.process_output(config, short_name, opt_list,
                                              stdout_filename, stderr_filename)


class PythonMLlibTests(PerfTestSuite, MLlibTestHelper):

    @classmethod
    def get_spark_submit_cmd(cls, cluster, config, main_class_or_script, opt_list, stdout_filename,
                             stderr_filename):
        spark_submit = "%s/bin/spark-submit" % cluster.spark_home
        cmd = "%s --master %s pyspark-tests/%s %s 1>> %s 2>> %s" % (
            spark_submit, config.SPARK_CLUSTER_URL,
            main_class_or_script, " ".join(opt_list),
            stdout_filename, stderr_filename)
        return cmd

    @classmethod
    def process_output(cls, config, short_name, opt_list, stdout_filename, stderr_filename):
        return MLlibTestHelper.process_output(config, short_name, opt_list,
                                              stdout_filename, stderr_filename)


class PythonTests(PerfTestSuite):

    @classmethod
    def get_spark_submit_cmd(cls, cluster, config, main_class_or_script, opt_list, stdout_filename,
                             stderr_filename):
        spark_submit = "%s/bin/spark-submit" % cluster.spark_home
        cmd = "%s --master %s pyspark-tests/%s %s 1>> %s 2>> %s" % (
            spark_submit, config.SPARK_CLUSTER_URL, main_class_or_script, " ".join(opt_list),
            stdout_filename, stderr_filename)
        return cmd

    @classmethod
    def process_output(cls, config, short_name, opt_list, stdout_filename, stderr_filename):
        with open(stdout_filename, "r") as stdout_file:
            output = stdout_file.read()
        results_token = "results: "
        if results_token not in output:
            print("Test did not produce expected results. Output was:")
            print(output)
            sys.exit(1)
        result_line = filter(lambda x: results_token in x, output.split("\n"))[-1]
        result_list = result_line.replace(results_token, "").split(",")
        err_msg = ("Expecting at least %s results "
                   "but only found %s" % (config.IGNORED_TRIALS + 1, len(result_list)))
        assert len(result_list) > config.IGNORED_TRIALS, err_msg
        result_list = result_list[config.IGNORED_TRIALS:]

        result_string = "%s, %s, " % (short_name, " ".join(opt_list))

        result_string += "%s, %.3f, %s, %s, %s\n" % stats_for_results(result_list)

        sys.stdout.flush()
        return result_string
