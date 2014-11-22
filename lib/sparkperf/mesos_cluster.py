from sparkperf.cluster import Cluster
import json
import os
import urllib2


class MesosCluster(Cluster):
    """
    Functionality for interacting with a already running Mesos Spark cluster.
    All the behavior for starting and stopping the cluster is not supported.
    """

    def __init__(self, spark_home, mesos_master, spark_conf_dir = None, commit_sha="unknown"):
        self.spark_home = spark_home
        self.mesos_master = mesos_master
        self.spark_conf_dir = spark_conf_dir
        self.commit_sha = commit_sha

        state_url = "http://" + os.path.join(mesos_master.strip("mesos://"), "state.json")
        resp = urllib2.urlopen(state_url)
        if resp.getcode() != 200:
            raise "Bad status code returned fetching state.json from mesos master"

        state = json.loads(resp.read())
        self.slaves = list(map((lambda slave: slave["hostname"]), state["slaves"]))

    def sync_spark(self):
        None

    def stop(self):
        None

    def start(self):
        None

    def ensure_spark_stopped_on_slaves(self):
        None



