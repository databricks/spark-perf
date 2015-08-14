import time
import random
import json
import sys

import pyspark


class DataGenerator:

    def generateIntData(self, sc, records, uniqueKeys, uniqueValues, numPartitions, seed):
        n = records / numPartitions
        def gen(index):
            ran = random.Random(hash(str(seed ^ index)))
            for i in xrange(n):
                yield ran.randint(0, uniqueKeys), ran.randint(0, uniqueValues)
        return sc.parallelize(xrange(numPartitions), numPartitions).flatMap(gen)

    def createKVDataSet(self, sc, dataType, records, uniqueKeys, uniqueValues, keyLength,
                        valueLength, numPartitions, seed,
                        persistenceType):
        inputRDD = self.generateIntData(sc, records, uniqueKeys, uniqueValues, numPartitions, seed)
        keyfmt = "%%0%dd" % keyLength
        valuefmt = "%%0%dd" % valueLength
        if dataType == "string":
            inputRDD = inputRDD.map(lambda (k, v): (keyfmt % k, valuefmt % v)) 
        if persistenceType == "memory":
            rdd = inputRDD.persist(pyspark.StorageLevel.MEMORY_ONLY)
        elif persistenceType == "disk":
            rdd = inputRDD.persist(pyspark.StorageLevel.DISK_ONLY)
        elif persistenceType == "hdfs":
            pass
        rdd.count()
        return rdd


class PerfTest(object):
    def __init__(self, sc):
        self.sc = sc

    def initialize(self, options):
        self.options = options

    def createInputData(self):
        pass

    def runTest(self):
        raise NotImplementedError

    def run(self):
        options = self.options
        rs = []
        for i in range(options.num_trials):
            start = time.time()
            self.runTest()
            rs.append(time.time() - start)
            time.sleep(options.inter_trial_wait)
        return rs


class SchedulerThroughputTest(PerfTest):
    def runTest(self):
        self.sc.parallelize(xrange(options.num_tasks), options.num_tasks).count()
      

class KVDataTest(PerfTest):
    def __init__(self, sc, dataType="string"):
        PerfTest.__init__(self, sc)
        self.dataType = dataType

    def createInputData(self):
        options = self.options
        self.rdd = DataGenerator().createKVDataSet(
                self.sc, self.dataType, options.num_records,
                options.unique_keys, options.unique_values,
                options.key_length, options.value_length,
                options.num_partitions, options.random_seed,
                options.persistent_type)


class KVDataTestInt(KVDataTest):
    def __init__(self, sc):
        KVDataTest.__init__(self, sc, "int")

class AggregateByKey(KVDataTest):
    def runTest(self):
        self.rdd.map(lambda (k, v): (k, int(v))).reduceByKey(lambda x, y: x + y, self.options.reduce_tasks).count()

class AggregateByKeyInt(KVDataTestInt):
    def runTest(self):
        self.rdd.reduceByKey(lambda x, y: x + y, self.options.reduce_tasks).count()

class AggregateByKeyNaive(KVDataTest):
    def runTest(self):
        self.rdd.map(lambda (k, v): (k, int(v))).groupByKey(self.options.reduce_tasks).mapValues(sum).count()

class SortByKey(KVDataTest):
    def runTest(self):
        self.rdd.sortByKey(numPartitions=self.options.reduce_tasks).count()

class SortByKeyInt(KVDataTestInt):
    def runTest(self):
        self.rdd.sortByKey(numPartitions=self.options.reduce_tasks).count()

class Count(KVDataTest):
    def runTest(self):
        self.rdd.count()

class CountWithFilter(KVDataTest):
    def runTest(self):
        self.rdd.filter(lambda (k, v): int(v) % 2).count()

class BroadcastWithBytes(PerfTest):
    def createInputData(self):
        n = self.options.broadcast_size
        if n > (1 << 20):
            block = open("/dev/urandom").read(1 << 20)
            self.data = block * (n >> 20)
        else:
            self.data = open("/dev/urandom").read(n)
    def runTest(self):
        n = len(self.data)
        s = self.sc.broadcast(self.data)
        rdd = self.sc.parallelize(range(self.options.num_partitions), 100)
        assert rdd.filter(lambda x: len(s.value) == n).count() == self.options.num_partitions
        s.unpersist()
   
class BroadcastWithSet(BroadcastWithBytes):
    def createInputData(self):
        n = self.options.broadcast_size / 32
        self.data = set(range(n))


all_tests = [
    "AggregateByKey",
    "AggregateByKeyInt",
    "AggregateByKeyNaive",
    "BroadcastWithBytes",
    "BroadcastWithSet",
    "Count",
    "CountWithFilter",
    "SchedulerThroughputTest",
    "SortByKey",
    "SortByKeyInt",
]


if __name__ == "__main__":
    import optparse
    parser = optparse.OptionParser(usage="Usage: %prog [options] test_names")
    parser.add_option("--num-trials", type="int", default=1)
    parser.add_option("--num-tasks", type="int", default=4)
    parser.add_option("--reduce-tasks", type="int", default=4)
    parser.add_option("--num-records", type="int", default=1024)
    parser.add_option("--inter-trial-wait", type="int", default=0)
    parser.add_option("--unique-keys", type="int", default=1024)
    parser.add_option("--key-length", type="int", default=10)
    parser.add_option("--unique-values", type="int", default=102400)
    parser.add_option("--value-length", type="int", default=20)
    parser.add_option("--num-partitions", type="int", default=10)
    parser.add_option("--broadcast-size", type="int", default=1 << 20)
    parser.add_option("--random-seed", type="int", default=1)
    parser.add_option("--storage-location", type="str", default="/")
    parser.add_option("--persistent-type", default="memory")
    parser.add_option("--wait-for-exit", action="store_true")

    parser.add_option("--list", "-l", action="store_true", help="list all tests")
    parser.add_option("--all", "-a", action="store_true", help="run all tests")

    options, cases = parser.parse_args()

    if options.list:
        for n in all_tests:
            print n
        sys.exit(0)

    if options.all:
        cases = all_tests

    sc = pyspark.SparkContext(appName="TestRunner")
    for name in cases:
        print 'run test:', name
        test = globals()[name](sc)
        test.initialize(options)
        test.createInputData()
        results = test.run()
        print "results:", ",".join("%.3f" % t for t in results)
        
        # JSON results
        javaSystemProperties = sc._jvm.System.getProperties()
        systemProperties = {}
        for k in javaSystemProperties.keys():
            systemProperties[k] = str(javaSystemProperties[k])
        sparkConfInfo = {} # convert to dict to match Scala JSON
        for (a,b) in sc._conf.getAll():
            sparkConfInfo[a] = b
        jsonResults = json.dumps({"testName": name,
                                  "options": vars(options),
                                  "sparkConf": sparkConfInfo,
                                  "sparkVersion": sc.version,
                                  "systemProperties": systemProperties,
                                  "results": results,
                                  "bestResult:": min(results)},
                                 separators=(',', ':'))  # use separators for compact encoding
        print "jsonResults: " + jsonResults
