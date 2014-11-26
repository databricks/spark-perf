import os
import os.path
from subprocess import Popen
import sys
import threading
from contextlib import contextmanager

SBT_CMD = "sbt/sbt"

@contextmanager
def cd(target_dir):
    """
    Context manager for switching directories and automatically changing back to the old one.
    >>> x = os.getcwd()
    >>> with cd("/"):
    ...     print os.getcwd()
    /
    >>> os.getcwd() == x
    True
    """
    old_wd = os.getcwd()
    try:
        os.chdir(target_dir)
        yield
    finally:
        os.chdir(old_wd)

def run_cmd(cmd, exit_on_fail=True):
    """
    Run a shell command and ignore its output.
    """
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


def run_cmds_parallel(commands):
    """
    Run several commands in parallel, waiting for them all to finish.
    :param commands: an array of tuples, where each tuple consists of (command_name, exit_on_fail)
    """
    threads = []
    results = [None] * len(commands)
    
    def run_cmd_in_thread(i, cmd,  exit_on_fail):
        return_code = run_cmd(cmd, exit_on_fail)
        results[i] = return_code

    for i, (cmd_name, exit_on_fail) in enumerate(commands):
        thread = threading.Thread(target=run_cmd_in_thread, args=(i, cmd_name, exit_on_fail))
        thread.daemon = True
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
    return results

def make_ssh_cmd(cmd_name, host):
    """
    Return a command running `cmd_name` on `host` with proper SSH configs.
    """
    return "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 %s '%s'" % (host, cmd_name)


def make_rsync_cmd(dir_name, host):
    """
    Return a command which copies the supplied directory to the given host.
    """
    return ('rsync --delete -e "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5" -az "%s/" '
            '"%s:%s"') % (dir_name, host, os.path.abspath(dir_name))


def clear_dir(dir_name, hosts, prompt_for_deletes):
    """
    Delete all files in the given directory on the specified hosts.
    """
    err_msg = ("Attempted to delete directory '%s/*', halting "
               "rather than deleting entire file system.") % dir_name
    assert dir_name != "" and dir_name != "/", err_msg
    if prompt_for_deletes:
        response = raw_input("\nAbout to remove all files and directories under %s on %s, is "
                             "this ok? [y, n] " % (dir_name, hosts))
        if response != "y":
            return
    run_cmds_parallel([(make_ssh_cmd("rm -r %s/*" % dir_name, host), False) for host in hosts])
