"""Configuration options for running Spark and Shark performance tests (perf-tests)."""

import time
import os
import os.path
import socket

from sparkperf.config_utils import FlagSet, JavaOptionSet, OptionSet, ConstantOption

# Standard Configuration Options
# --------------------------------------------------------------------------------------------------

# Point to an installation of Spark on the cluster.
SPARK_HOME_DIR = "/root/spark"
# TODO: If we drop the disk warm-up, we could limit the scope of this to only when using
#       the cluster Spark.

# Master used when submitting Spark jobs.
SPARK_CLUSTER_URL = open("/root/spark-ec2/cluster-url", 'r').readline().strip() # EC2 Cluster
# SPARK_CLUSTER_URL = "spark://%s:7077" % socket.gethostname()                  # Local cluster

# If this is true, we'll submit your job using an existing Spark installation.
# If this is false, we'll checkout and build a specific version of Spark, and
# copy configurations from your existing Spark install.
USE_CLUSTER_SPARK = True

# URL of the HDFS installation in the Spark EC2 cluster
HDFS_URL = "hdfs://%s:9000/test/" % socket.gethostname()
# HDFS_URL = "%s/test/" % os.getcwd()

############## If not using existing Spark installation  ############
# Commit id and repo used if you are not using an existing Spark cluster
# custom version of Spark. The remote name in your git repo is assumed 
# to be "origin". 
#
# The commit ID can specify any of the following:
#     1. A git commit hash         e.g. "4af93ff3"
#     2. A branch name             e.g. "origin/branch-0.7"
#     3. A tag name                e.g. "origin/tag/v0.8.0-incubating"
#     4. A pull request            e.g. "origin/pr/675"
SPARK_COMMIT_ID = ""
SPARK_GIT_REPO = "https://github.com/apache/spark.git"
SPARK_MERGE_COMMIT_INTO_MASTER = False # Whether to merge the commit into master
#####################################################################

# File to write results to.
SPARK_OUTPUT_FILENAME = "results/spark_perf_output_%s_%s" % (
    SPARK_COMMIT_ID.replace("/", "-"), time.strftime("%Y-%m-%d_%H-%M-%S"))
PYSPARK_OUTPUT_FILENAME = "results/python_perf_output_%s_%s" % (
    SPARK_COMMIT_ID.replace("/", "-"), time.strftime("%Y-%m-%d_%H-%M-%S"))
STREAMING_OUTPUT_FILENAME = "results/streaming_perf_output_%s_%s" % (
    SPARK_COMMIT_ID.replace("/", "-"), time.strftime("%Y-%m-%d_%H-%M-%S"))
MLLIB_OUTPUT_FILENAME = "results/mllib_perf_output_%s_%s" % (
    SPARK_COMMIT_ID.replace("/", "-"), time.strftime("%Y-%m-%d_%H-%M-%S"))

# Test Configuration
# --------------------------------------------------------------------------------------------------

# The default values configured below are appropriate for approximately 20 m1.xlarge nodes, 
# in which each node has 15 GB of memory. Use this variable to scale the values (e.g. 
# number of records in a generated dataset) if you are running the tests with more 
# or fewer nodes.
SCALE_FACTOR = 1.0

assert SCALE_FACTOR > 0, "SCALE_FACTOR must be > 0."

# If set, removes the first N trials for each test from all reported statistics. Useful for
# tests which have outlier behavior due to JIT and other system cache warm-ups. If any test
# returns fewer N + 1 results, an exception is thrown.
IGNORED_TRIALS = 1

# Command used to launch Scala or Java.

# Set up OptionSets. Note that giant cross product is done over all JavaOptionsSets + OptionSets
# passed to each test which may be combinations of those set up here.

# Java options.
COMMON_JAVA_OPTS = [
    # Fraction of JVM memory used for caching RDDs.
    JavaOptionSet("spark.storage.memoryFraction", [0.66]),
    JavaOptionSet("spark.serializer", ["org.apache.spark.serializer.JavaSerializer"]),
    JavaOptionSet("spark.executor.memory", ["9g"]),
    # Turn event logging on in order better diagnose failed tests. Off by default as it crashes
    # releases prior to 1.0.2
    # JavaOptionSet("spark.eventLog.enabled", [True]),
    # To ensure consistency across runs, we disable delay scheduling
    JavaOptionSet("spark.locality.wait", [str(60 * 1000 * 1000)])
]
# Set driver memory here
SPARK_DRIVER_MEMORY = "20g"
# The following options value sets are shared among all tests.
COMMON_OPTS = [
    # How many times to run each experiment - used to warm up system caches.
    # This OptionSet should probably only have a single value (i.e., length 1)
    # since it doesn't make sense to have multiple values here.
    OptionSet("num-trials", [5]),
    # Extra pause added between trials, in seconds. For runs with large amounts
    # of shuffle data, this gives time for buffer cache write-back.
    OptionSet("inter-trial-wait", [3])
]

# The following options value sets are shared among all tests of
# operations on key-value data.
SPARK_KEY_VAL_TEST_OPTS = [
    # The number of input partitions.
    OptionSet("num-partitions", [400], can_scale=True),
    # The number of reduce tasks.
    OptionSet("reduce-tasks", [400], can_scale=True),
    # A random seed to make tests reproducable.
    OptionSet("random-seed", [5]),
    # Input persistence strategy (can be "memory", "disk", or "hdfs").
    # NOTE: If "hdfs" is selected, datasets will be re-used across runs of
    #       this script. This means parameters here are effectively ignored if
    #       an existing input dataset is present.
    OptionSet("persistent-type", ["memory"]),
    # Whether to wait for input in order to exit the JVM.
    FlagSet("wait-for-exit", [False]),
    # Total number of records to create.
    OptionSet("num-records", [200 * 1000 * 1000], True),
    # Number of unique keys to sample from.
    OptionSet("unique-keys",[20 * 1000], True),
    # Length in characters of each key.
    OptionSet("key-length", [10]),
    # Number of unique values to sample from.
    OptionSet("unique-values", [1000 * 1000], True),
    # Length in characters of each value.
    OptionSet("value-length", [10]),
    # Use hashes instead of padded numbers for keys and values 
    FlagSet("hash-records", [False]),
    # Storage location if HDFS persistence is used
    OptionSet("storage-location", [
        os.getenv("HDFS_URL", "") + "/spark-perf-kv-data"])
]

# Test setup
# --------------------------------------------------------------------------------------------------
# Set up the actual tests. Each test is represtented by a tuple: (short_name, test_cmd,
# scale_factor, list<JavaOptionSet>, list<OptionSet>).

### Uncomment individual tests to add them

# Spark core tests #

SPARK_KV_OPTS = COMMON_OPTS + SPARK_KEY_VAL_TEST_OPTS
SPARK_TESTS = []

#SPARK_TESTS += [("scheduling-throughput", "spark.perf.TestRunner",
#    SCALE_FACTOR, COMMON_JAVA_OPTS, 
#    [ConstantOption("scheduling-throughput"), OptionSet("num-tasks", [10 * 1000])] + COMMON_OPTS)]

#SPARK_TESTS += [("scala-agg-by-key", "spark.perf.TestRunner", SCALE_FACTOR,
#    COMMON_JAVA_OPTS, [ConstantOption("aggregate-by-key")] + SPARK_KV_OPTS)]

# Scale the input for this test by 2x since ints are smaller.
#SPARK_TESTS += [("scala-agg-by-key-int", "spark.perf.TestRunner", SCALE_FACTOR * 2, 
#    COMMON_JAVA_OPTS, [ConstantOption("aggregate-by-key-int")] + SPARK_KV_OPTS)]

#SPARK_TESTS += [("scala-agg-by-key-naive", "spark.perf.TestRunner", SCALE_FACTOR,
#    COMMON_JAVA_OPTS, [ConstantOption("aggregate-by-key-naive")] + SPARK_KV_OPTS)]

# Scale the input for this test by 0.10.
#SPARK_TESTS += [("scala-sort-by-key", "spark.perf.TestRunner", SCALE_FACTOR * 0.1,
#    COMMON_JAVA_OPTS, [ConstantOption("sort-by-key")] + SPARK_KV_OPTS)]

#SPARK_TESTS += [("scala-sort-by-key-int", "spark.perf.TestRunner", SCALE_FACTOR * 0.2,
#    COMMON_JAVA_OPTS, [ConstantOption("sort-by-key-int")] + SPARK_KV_OPTS)]

#SPARK_TESTS += [("scala-count", "spark.perf.TestRunner", SCALE_FACTOR, 
#    COMMON_JAVA_OPTS, [ConstantOption("count")] + SPARK_KV_OPTS)]

#SPARK_TESTS += [("scala-count-w-fltr", "spark.perf.TestRunner", SCALE_FACTOR,
#    COMMON_JAVA_OPTS, [ConstantOption("count-with-filter")] + SPARK_KV_OPTS)]



# PySpark tests
PYSPARK_TESTS = []

#PYSPARK_TESTS += [("python-scheduling-throughput", "tests.py",
#    SCALE_FACTOR, COMMON_JAVA_OPTS,
#    [ConstantOption("SchedulerThroughputTest"), OptionSet("num-tasks", [5000])] + COMMON_OPTS)]

#PYSPARK_TESTS += [("python-agg-by-key", "tests.py", SCALE_FACTOR,
#    COMMON_JAVA_OPTS, [ConstantOption("AggregateByKey")] + SPARK_KV_OPTS)]

# Scale the input for this test by 2x since ints are smaller.
#PYSPARK_TESTS += [("python-agg-by-key-int", "tests.py", SCALE_FACTOR * 2,
#    COMMON_JAVA_OPTS, [ConstantOption("AggregateByKeyInt")] + SPARK_KV_OPTS)]

#PYSPARK_TESTS += [("python-agg-by-key-naive", "tests.py", SCALE_FACTOR,
#    COMMON_JAVA_OPTS, [ConstantOption("AggregateByKeyNaive")] + SPARK_KV_OPTS)]

## Scale the input for this test by 0.10.
#PYSPARK_TESTS += [("python-sort-by-key", "tests.py", SCALE_FACTOR * 0.1,
#    COMMON_JAVA_OPTS, [ConstantOption("SortByKey")] + SPARK_KV_OPTS)]

#PYSPARK_TESTS += [("python-sort-by-key-int", "tests.py", SCALE_FACTOR * 0.2,
#    COMMON_JAVA_OPTS, [ConstantOption("SortByKeyInt")] + SPARK_KV_OPTS)]

#PYSPARK_TESTS += [("python-count", "tests.py", SCALE_FACTOR,
#                 COMMON_JAVA_OPTS, [ConstantOption("Count")] + SPARK_KV_OPTS)]

#PYSPARK_TESTS += [("python-count-w-fltr", "tests.py", SCALE_FACTOR,
#    COMMON_JAVA_OPTS, [ConstantOption("CountWithFilter")] + SPARK_KV_OPTS)]



# Spark Streaming tests
STREAMING_TESTS = []

# The following function generates options for setting batch duration in streaming tests
def streaming_batch_duration_opts(duration):
    return [OptionSet("batch-duration", [duration])]

# The following function generates options for setting window duration in streaming tests
def streaming_window_duration_opts(duration):
    return [OptionSet("window-duration", [duration])]

STREAMING_COMMON_OPTS = [
    OptionSet("total-duration", [60]),
    OptionSet("hdfs-url", [HDFS_URL]),
]


STREAMING_COMMON_JAVA_OPTS = [
    # Fraction of JVM memory used for caching RDDs.
    JavaOptionSet("spark.storage.memoryFraction", [0.66]),
    JavaOptionSet("spark.serializer", ["org.apache.spark.serializer.JavaSerializer"]),
    JavaOptionSet("spark.executor.memory", ["9g"]),
    JavaOptionSet("spark.executor.extraJavaOptions", [" -XX:+UseConcMarkSweepGC "])
]
STREAMING_KEY_VAL_TEST_OPTS = STREAMING_COMMON_OPTS + streaming_batch_duration_opts(2000) + [
    # Number of input streams.
    OptionSet("num-streams", [1], can_scale=True),
    # Number of records per second per input stream
    OptionSet("records-per-sec", [10 * 1000]),
    # Number of reduce tasks.
    OptionSet("reduce-tasks", [10], can_scale=True),
    # memory serialization ("true" or "false").
    OptionSet("memory-serialization", ["true"]),
    # Number of unique keys to sample from.
    OptionSet("unique-keys",[100 * 1000], can_scale=True),
    # Length in characters of each key.
    OptionSet("unique-values", [1000 * 1000], can_scale=True),
]

STREAMING_HDFS_RECOVERY_TEST_OPTS = STREAMING_COMMON_OPTS + streaming_batch_duration_opts(5000) + [
    OptionSet("records-per-file", [10000]),
    OptionSet("file-cleaner-delay", [300])
]

# this test is just to see if everything is setup properly
#STREAMING_TESTS += [("basic", "streaming.perf.TestRunner", SCALE_FACTOR,
#    STREAMING_COMMON_JAVA_OPTS, [ConstantOption("basic")] + STREAMING_COMMON_OPTS + streaming_batch_duration_opts(1000))] 

#STREAMING_TESTS += [("state-by-key", "streaming.perf.TestRunner", SCALE_FACTOR,
#    STREAMING_COMMON_JAVA_OPTS, [ConstantOption("state-by-key")] + STREAMING_KEY_VAL_TEST_OPTS)]

#STREAMING_TESTS += [("group-by-key-and-window", "streaming.perf.TestRunner", SCALE_FACTOR,
#    STREAMING_COMMON_JAVA_OPTS, [ConstantOption("group-by-key-and-window")] + STREAMING_KEY_VAL_TEST_OPTS + streaming_window_duration_opts(10000) )]

#STREAMING_TESTS += [("reduce-by-key-and-window", "streaming.perf.TestRunner", SCALE_FACTOR,
#    STREAMING_COMMON_JAVA_OPTS, [ConstantOption("reduce-by-key-and-window")] + STREAMING_KEY_VAL_TEST_OPTS + streaming_window_duration_opts(10000) )]

#STREAMING_TESTS += [("hdfs-recovery", "streaming.perf.TestRunner hdfs-recovery", SCALE_FACTOR,
#    STREAMING_COMMON_JAVA_OPTS, [ConstantOption("hdfs-recovery")] + STREAMING_HDFS_RECOVERY_TEST_OPTS)]



# MLlib Tests #
MLLIB_TESTS = []

# Set this to 1.0, 1.1, 1.2 to test MLlib with a particular Spark version.
# Note: You should also edit the dependency in MLlibTestsBuild.scala.
MLLIB_SPARK_VERSION = 1.2
if MLLIB_SPARK_VERSION == 1.0:
    MLLIB_SPARK_VERSION_STR = "onepointoh"
elif MLLIB_SPARK_VERSION == 1.1:
    MLLIB_SPARK_VERSION_STR = "onepointone"
elif MLLIB_SPARK_VERSION == 1.2:
    MLLIB_SPARK_VERSION_STR = "onepointtwo"
else:
    raise Exception("Bad MLLIB_SPARK_VERSION: %r" % MLLIB_SPARK_VERSION)

# The following options value sets are shared among all tests of
# operations on MLlib algorithms.
MLLIB_COMMON_OPTS = COMMON_OPTS + [
    # The number of input partitions.
    OptionSet("num-partitions", [128], can_scale=True),
    # A random seed to make tests reproducable.
    OptionSet("random-seed", [5])
]

# Algorithms available in Spark-1.0 #

# Regression and Classification Tests #
MLLIB_REGRESSION_CLASSIFICATION_TEST_OPTS = MLLIB_COMMON_OPTS + [
    # The number of rows or examples
    OptionSet("num-examples", [1000000], can_scale=True),
    # The number of features per example
    OptionSet("num-features", [10000], can_scale=True)
]

# Generalized Linear Model (GLM) Tests #
MLLIB_GLM_TEST_OPTS = MLLIB_REGRESSION_CLASSIFICATION_TEST_OPTS + [
    # The number of iterations for SGD
    OptionSet("num-iterations", [20]),
    # The step size for SGD
    OptionSet("step-size", [0.05]),
    # Regularization type: none, L1, L2
    OptionSet("reg-type", ["none", "L1", "L2"]),
    # Regularization parameter
    OptionSet("reg-param", [0.1])
]
if MLLIB_SPARK_VERSION >= 1.1:
    MLLIB_GLM_TEST_OPTS += [
        # Optimization algorithm: sgd, lbfgs
        OptionSet("optimizer", ["sgd"])
    ]

# GLM Regression Tests #
MLLIB_GLM_REGRESSION_TEST_OPTS = MLLIB_GLM_TEST_OPTS + [
    # The intercept for the data
    OptionSet("intercept", [0.0]),
    # The scale factor for the noise
    OptionSet("epsilon", [0.1]),
    # Loss to minimize: L2 (squared error)
    OptionSet("loss", ["L2"])
]

MLLIB_TESTS += [("glm-regression", "mllib.perf." + MLLIB_SPARK_VERSION_STR + ".TestRunner", SCALE_FACTOR,
    COMMON_JAVA_OPTS, [ConstantOption("glm-regression")] + MLLIB_GLM_REGRESSION_TEST_OPTS)]

# Classification Tests #
MLLIB_CLASSIFICATION_TEST_OPTS = MLLIB_GLM_TEST_OPTS + [
     # Expected fraction of examples which are negative
     OptionSet("per-negative", [0.3]),
     # The scale factor for the noise in feature values
     OptionSet("scale-factor", [1.0])
]

# GLM Classification Tests #
MLLIB_GLM_CLASSIFICATION_TEST_OPTS = MLLIB_CLASSIFICATION_TEST_OPTS + [
    # Loss to minimize: logistic, hinge (SVM)
    OptionSet("loss", ["logistic", "hinge"])
]

MLLIB_TESTS += [("glm-classification", "mllib.perf." + MLLIB_SPARK_VERSION_STR + ".TestRunner", SCALE_FACTOR,
    COMMON_JAVA_OPTS, [ConstantOption("glm-classification")] +
    MLLIB_GLM_CLASSIFICATION_TEST_OPTS)]

# Naive Bayes piggy-backs on classification for data generation.
NAIVE_BAYES_TEST_OPTS = MLLIB_CLASSIFICATION_TEST_OPTS + [
    # Naive Bayes smoothing lambda.
    OptionSet("nb-lambda", [1.0])
]

MLLIB_TESTS += [("naive-bayes", "mllib.perf." + MLLIB_SPARK_VERSION_STR + ".TestRunner", SCALE_FACTOR,
    COMMON_JAVA_OPTS, [ConstantOption("naive-bayes")] +
    NAIVE_BAYES_TEST_OPTS)]

# Decision Trees #
MLLIB_DECISION_TREE_TEST_OPTS = MLLIB_COMMON_OPTS + [
    # The number of rows or examples
    OptionSet("num-examples", [1000000], can_scale=True),
    # The number of features per example
    OptionSet("num-features", [500], can_scale=True),
    # Type of label: 0 indicates regression, 2+ indicates classification with this many classes
    # Note: multi-class (>2) is not supported in Spark 1.0.
    OptionSet("label-type", [0, 2], can_scale=False),
    # Fraction of features which are categorical
    OptionSet("frac-categorical-features", [0.5], can_scale=False),
    # Fraction of categorical features which are binary. Others have 20 categories.
    OptionSet("frac-binary-features", [0.5], can_scale=False),
    # Depth of true decision tree model used to label examples.
    # WARNING: The meaning of depth changed from Spark 1.0 to Spark 1.1:
    #          depth=N for Spark 1.0 should be depth=N-1 for Spark 1.1
    OptionSet("tree-depth", [1, 10], can_scale=False),
    # Maximum number of bins for the decision tree learning algorithm.
    OptionSet("max-bins", [32], can_scale=False),
]

if MLLIB_SPARK_VERSION >= 1.2:
    MLLIB_DECISION_TREE_TEST_OPTS += [
        # Ensemble type: RandomForest, GradientBoosting
        # Note: If num-trees = 1, then RandomForest and GradientBoosting should give the same result.
        OptionSet("ensemble-type", ["RandomForest", "GradientBoosting"]),
        # Path to training dataset (if not given, use random data).
        OptionSet("training-data", [""]),
        # Path to test dataset (only used if training dataset given).
        # If not given, hold out part of training data for validation.
        OptionSet("test-data", [""]),
        # Fraction of data to hold out for testing (ignored if given training and test dataset).
        OptionSet("test-data-fraction", [0.2], can_scale=False),
        # Number of trees. If 1, then run DecisionTree. If >1, then run RandomForest.
        OptionSet("num-trees", [1, 10], can_scale=False),
        # Feature subset sampling strategy: auto, all, sqrt, log2, onethird
        # (only used for RandomForest)
        OptionSet("feature-subset-strategy", ["auto"])
    ]

MLLIB_TESTS += [("decision-tree", "mllib.perf." + MLLIB_SPARK_VERSION_STR + ".TestRunner", SCALE_FACTOR,
    COMMON_JAVA_OPTS, [ConstantOption("decision-tree")] +
    MLLIB_DECISION_TREE_TEST_OPTS)]

# Recommendation Tests #
MLLIB_RECOMMENDATION_TEST_OPTS = MLLIB_COMMON_OPTS + [
     # The number of users
     OptionSet("num-users", [6000000], can_scale=True),
     # The number of products
     OptionSet("num-products", [5000000], can_scale=True),
     # The number of ratings
     OptionSet("num-ratings", [100000000], can_scale=True),
     # The number of iterations for ALS
     OptionSet("num-iterations", [20]),
     # The rank of the factorized matrix model
     OptionSet("rank", [20]),
     # The regularization parameter
     OptionSet("reg-param", [1.0]),
     # Whether to use implicit preferences or not
     FlagSet("implicit-prefs", [False])
]

MLLIB_TESTS += [("als", "mllib.perf." + MLLIB_SPARK_VERSION_STR + ".TestRunner", SCALE_FACTOR,
    COMMON_JAVA_OPTS, [ConstantOption("als")] +
    MLLIB_RECOMMENDATION_TEST_OPTS)]

# Clustering Tests #
MLLIB_CLUSTERING_TEST_OPTS = MLLIB_COMMON_OPTS + [
     # The number of points
     OptionSet("num-points", [1000000], can_scale=True),
     # The number of features per point
     OptionSet("num-columns", [10000], can_scale=True),
     # The number of centers
     OptionSet("num-centers", [20]),
     # The number of iterations for KMeans
     OptionSet("num-iterations", [20])
]

MLLIB_TESTS += [("kmeans", "mllib.perf." + MLLIB_SPARK_VERSION_STR + ".TestRunner", SCALE_FACTOR,
    COMMON_JAVA_OPTS, [ConstantOption("kmeans")] + MLLIB_CLUSTERING_TEST_OPTS)]

# Linear Algebra Tests #
MLLIB_LINALG_TEST_OPTS = MLLIB_COMMON_OPTS + [
     # The number of rows for the matrix
     OptionSet("num-rows", [1000000], can_scale=True),
     # The number of columns for the matrix
     OptionSet("num-cols", [1000], can_scale=True),
     # The number of top singular values wanted for SVD and PCA
     OptionSet("rank", [50], can_scale=True)
]

MLLIB_TESTS += [("svd", "mllib.perf." + MLLIB_SPARK_VERSION_STR + ".TestRunner", SCALE_FACTOR,
    COMMON_JAVA_OPTS, [ConstantOption("svd")] + MLLIB_LINALG_TEST_OPTS)]

MLLIB_TESTS += [("pca", "mllib.perf." + MLLIB_SPARK_VERSION_STR + ".TestRunner", SCALE_FACTOR,
    COMMON_JAVA_OPTS, [ConstantOption("pca")] + MLLIB_LINALG_TEST_OPTS)]

MLLIB_TESTS += [("summary-statistics", "mllib.perf." + MLLIB_SPARK_VERSION_STR + ".TestRunner", SCALE_FACTOR,
    COMMON_JAVA_OPTS, [ConstantOption("summary-statistics")] +
    MLLIB_LINALG_TEST_OPTS)]

# Statistic Toolkit Tests #
if MLLIB_SPARK_VERSION >= 1.1:
    MLLIB_STATS_TEST_OPTS = MLLIB_COMMON_OPTS

    MLLIB_TESTS += [("pearson", "mllib.perf." + MLLIB_SPARK_VERSION_STR + ".TestRunner", SCALE_FACTOR,
        COMMON_JAVA_OPTS, [ConstantOption("pearson"),
        OptionSet("num-rows", [10000000], can_scale=True), OptionSet("num-cols", [1000], can_scale=True)] +
        MLLIB_STATS_TEST_OPTS)]

    MLLIB_TESTS += [("spearman", "mllib.perf." + MLLIB_SPARK_VERSION_STR + ".TestRunner", SCALE_FACTOR,
        COMMON_JAVA_OPTS, [ConstantOption("spearman"),
        OptionSet("num-rows", [10000000], can_scale=True), OptionSet("num-cols", [100], can_scale=True)] +
        MLLIB_STATS_TEST_OPTS)]

    MLLIB_TESTS += [("chi-sq-feature", "mllib.perf." + MLLIB_SPARK_VERSION_STR + ".TestRunner", SCALE_FACTOR,
        COMMON_JAVA_OPTS, [ConstantOption("chi-sq-feature"),
        OptionSet("num-rows", [10000000], can_scale=True), OptionSet("num-cols", [500], can_scale=True)] +
        MLLIB_STATS_TEST_OPTS)]

    MLLIB_TESTS += [("chi-sq-gof", "mllib.perf." + MLLIB_SPARK_VERSION_STR + ".TestRunner", SCALE_FACTOR,
        COMMON_JAVA_OPTS, [ConstantOption("chi-sq-gof"),
        OptionSet("num-rows", [50000000], can_scale=True), OptionSet("num-cols", [0], can_scale=True)] +
        MLLIB_STATS_TEST_OPTS)]

    MLLIB_TESTS += [("chi-sq-mat", "mllib.perf." + MLLIB_SPARK_VERSION_STR + ".TestRunner", SCALE_FACTOR,
        COMMON_JAVA_OPTS, [ConstantOption("chi-sq-mat"),
        OptionSet("num-rows", [20000], can_scale=True), OptionSet("num-cols", [0], can_scale=True)] +
        MLLIB_STATS_TEST_OPTS)]

# Skip downloading and building Spark
SPARK_SKIP_PREP = False

SPARK_SKIP_TESTS = False
STREAMING_SKIP_TESTS = False
MLLIB_SKIP_TESTS = False

# Skip building and packaging project tests (requires respective perf tests to already be packaged
# in the project's target directory).
SPARK_SKIP_TEST_PREP = False
STREAMING_SKIP_TEST_PREP = False
MLLIB_SKIP_TEST_PREP = False

# Skip warming up local disks (warm-up is only necesary on EC2).
SKIP_DISK_WARMUP = True

# Total number of bytes used to warm up each local directory.
DISK_WARMUP_BYTES = 200 * 1024 * 1024

# Number of files to create when warming up each local directory. Bytes will be evenly divided
# across files.
DISK_WARMUP_FILES = 200

# Prompt for confirmation when deleting temporary files.
PROMPT_FOR_DELETES = True

# Use a custom configuration directory
SPARK_CONF_DIR = SPARK_HOME_DIR + "/conf"
