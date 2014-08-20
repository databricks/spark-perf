from sparkperf.commands import run_cmd, make_ssh_cmd
import time


class Cluster(object):
    """
    Functionality for interacting with a Spark cluster.
    """

    def __init__(self, spark_home, spark_conf_dir=None):
        self.spark_home = spark_home
        self.spark_conf_dir = spark_conf_dir or "%s/conf" % spark_home


        # Get a list of slaves by parsing the slaves file in SPARK_CONF_DIR.
        slaves_file_raw = open("%s/slaves" % self.spark_conf_dir, 'r').read().split("\n")
        self.slaves = filter(lambda x: not x.startswith("#") and not x is "", slaves_file_raw)

    def stop(self):
        print "Stopping Spark cluster"
        run_cmd("%s/sbin/stop-all.sh" % self.spark_home)

    def start(self):
        run_cmd("%s/sbin/start-all.sh" % self.spark_home)

    def ensure_spark_stopped_on_slaves(self):
        """
        Ensures that no executors are running on Spark slaves. Executors can continue to run for some
        time after a shutdown signal is given due to cleaning up temporary files.
        """
        stop = False
        while not stop:
            cmd = "ps -ef | grep -v grep | grep ExecutorBackend"
            ret_vals = map(lambda s: run_cmd(make_ssh_cmd(cmd, s), False), self.slaves)
            if 0 in ret_vals:
                print "Spark is still running on some slaves ... sleeping for 10 seconds"
                time.sleep(10)
            else:
                stop = True
