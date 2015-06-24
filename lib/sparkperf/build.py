import os
from subprocess import Popen, PIPE
from sparkperf.commands import run_cmd, cd
from sparkperf.cluster import Cluster
import logging


logger = logging.getLogger("sparkperf.build")


def clone_spark(target_dir, spark_git_repo):
    # Assumes that the preexisting `target_dir` directory is valid.
    if not os.path.isdir(target_dir):
        logger.info("Git cloning Spark from %s" % spark_git_repo)
        run_cmd("git clone %s %s" % (spark_git_repo, target_dir))
        # Allow PRs and tags to be fetched:
        run_cmd(("cd %s; git config --add remote.origin.fetch "
                 "'+refs/pull/*/head:refs/remotes/origin/pr/*'") % target_dir)
        run_cmd(("cd %s; git config --add remote.origin.fetch "
                 "'+refs/tags/*:refs/remotes/origin/tag/*'") % target_dir)


def checkout_version(repo_dir, commit_id, merge_commit_into_master=False):
    with cd(repo_dir):
        # Fetch updates
        logger.info("Updating Spark repo...")
        run_cmd("git fetch")

        # Check out the requested commit / branch / PR
        logger.info("Cleaning Spark and checking out commit_id %s." % commit_id)
        run_cmd("git clean -f -d -x")

        if merge_commit_into_master:
            run_cmd("git reset --hard master")
            run_cmd("git merge %s -m ='Merging %s into master.'" %
                    (commit_id, commit_id))
        else:
            run_cmd("git reset --hard %s" % commit_id)


def make_spark_distribution(
        commit_id, target_dir, spark_git_repo,
        merge_commit_into_master=False, is_yarn_mode=False,
        additional_make_distribution_args=""):
    """
    Download Spark, check out a specific version, and create a binary distribution.

    :param commit_id: the version to build.  Can specify any of the following:
        1. A git commit hash         e.g. "4af93ff3"
        2. A branch name             e.g. "origin/branch-0.7"
        3. A tag name                e.g. "origin/tag/v0.8.0-incubating"
        4. A pull request            e.g. "origin/pr/675"
    :param target_dir: the directory to clone Spark into.
    :param merge_commit_into_master: if True, this commit_id will be merged into `master`; this can
                                     be useful for testing un-merged pull requests.
    :param spark_git_repo: the repo to clone from.  By default, this is the Spark GitHub mirror.
    """
    clone_spark(target_dir, spark_git_repo)
    checkout_version(target_dir, commit_id, merge_commit_into_master)
    with cd(target_dir):
        logger.info("Building spark at version %s; This may take a while...\n" % commit_id)
        # According to the SPARK-1520 JIRA, building with Java 7+ will only cause problems when
        # running PySpark on YARN or when running on Java 6.  Since we'll be building and running
        # Spark on the same machines and using standalone mode, it should be safe to
        # disable this warning:
        if is_yarn_mode:
            run_cmd("./make-distribution.sh --skip-java-test -Pyarn " + additional_make_distribution_args)
        else:
            run_cmd("./make-distribution.sh --skip-java-test " + additional_make_distribution_args)


def copy_configuration(conf_dir, target_dir):
    # Copy Spark configuration files to new directory.
    logger.info("Copying all files from %s to %s/conf/" % (conf_dir, target_dir))
    assert os.path.exists("%s/spark-env.sh" % conf_dir), \
        "Could not find required file %s/spark-env.sh" % conf_dir
    assert os.path.exists("%s/slaves" % conf_dir), \
        "Could not find required file %s/slaves" % conf_dir
    run_cmd("cp %s/* %s/conf/" % (conf_dir, target_dir))


class SparkBuildManager(object):
    """
    Manages a collection of Spark builds, using cached builds (if available) or by
    fetching and building Spark.
    """
    def __init__(self, root_dir, spark_git_repo="https://github.com/apache/spark.git"):
        self.root_dir = root_dir
        self.spark_git_repo = spark_git_repo
        self._master_spark = os.path.join(self.root_dir, "master")
        if not os.path.isdir(root_dir):
            os.makedirs(root_dir)

    def get_cluster(self, commit_id, conf_dir, merge_commit_into_master=False, is_yarn_mode=False, additional_make_distribution_args=""):
        if not os.path.isdir(self._master_spark):
            clone_spark(self._master_spark, self.spark_git_repo)
        # Get the SHA corresponding to the commit and check if we've already built this version:
        checkout_version(self._master_spark, commit_id, merge_commit_into_master)
        sha = Popen("cd %s; git rev-parse --verify HEAD" % self._master_spark, shell=True,
                     stdout=PIPE).communicate()[0]
        sha = sha.strip()
        logger.debug("Requested version %s corresponds to SHA %s" % (commit_id, sha))
        cluster_dir = os.path.join(self.root_dir, sha)
        # TODO: detect and recover from failed builds?
        logger.debug("Searching for dir %s" % cluster_dir)
        if os.path.exists(cluster_dir):
            logger.info("Found pre-compiled Spark with SHA %s; skipping build" % sha)
        else:
            logger.info("Could not find pre-compiled Spark with SHA %s" % sha)
            # Check out and build the requested version of Spark in the master spark directory
            make_spark_distribution(
                commit_id=commit_id, target_dir=self._master_spark, spark_git_repo=self.spark_git_repo,
                merge_commit_into_master=merge_commit_into_master, is_yarn_mode=is_yarn_mode,
                additional_make_distribution_args=additional_make_distribution_args)
            # Copy the completed build to a directory named after the SHA.
            run_cmd("mv %s %s" % (os.path.join(self._master_spark, "dist"), cluster_dir))
        copy_configuration(conf_dir, cluster_dir)
        return Cluster(spark_home=cluster_dir, spark_conf_dir=conf_dir, commit_sha=sha)
