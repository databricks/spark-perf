from sparkperf.commands import run_cmd, make_ssh_cmd, run_cmds_parallel, clear_dir
import re
import sys
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

    def warmup_disks(self, bytes_to_write, disk_warmup_files):
        """
        Warm up local disks (this is only necessary on EC2).

        :param bytes_to_write: Total number of bytes used to warm up each local directory.
        :param disk_warmup_files: Number of files to create when warming up each local directory.
                                  Bytes will be evenly divided across files.
        """
        # Search for 'spark.local.dir' in spark-env.sh.
        path_to_env_file = "%s/spark-env.sh" % self.spark_conf_dir
        env_file_content = open(path_to_env_file, 'r').read()
        re_result = re.search(r'SPARK_LOCAL_DIRS=(.*)', env_file_content)
        if re_result:
            spark_local_dirs = re_result.group(1).split(",")
        else:
            err_msg = \
                ("ERROR: These scripts require you to explicitly set SPARK_LOCAL_DIRS "
                 "in spark-env.sh so that it can be cleaned. The way we check this is pretty  "
                 "picky, specifically we try to find the following string in spark-env.sh: "
                 "SPARK_LOCAL_DIRS=ONE_OR_MORE_DIRNAMES\" so you will want a line like this: ")
            sys.exit(err_msg)

        for local_dir in spark_local_dirs:
            # Strip off any trailing whitespace(s) so that the clear commands can work properly:
            local_dir = local_dir.rstrip()

            bytes_per_file = bytes_to_write / disk_warmup_files
            gen_command = "dd if=/dev/urandom bs=%s count=1 | split -a 5 -b %s - %s/random" % (
                bytes_to_write, bytes_per_file, local_dir)
            # Ensures the directory exists.
            dir_command = "mkdir -p %s" % local_dir

            print("Generating test data for %s, this may take some time" % local_dir)
            all_hosts = self.slaves + ["localhost"]
            run_cmds_parallel([(make_ssh_cmd(dir_command, host), True) for host in all_hosts])
            run_cmds_parallel([(make_ssh_cmd(gen_command, host), True) for host in all_hosts])
            clear_dir(local_dir, all_hosts, prompt_for_deletes=False)