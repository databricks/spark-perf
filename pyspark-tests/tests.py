import time
import random

import pyspark


class DataGenerator:

    def paddedString(self, i, length, doHash=False):
        return ("%%0%dd" % length) % (hash(i) if doHash else i)

    def generateIntData(self, sc, records, uniqueKeys, uniqueValues, numPartitions, seed):
        n = records / numPartitions
        def gen(index):
            ran = random.Random(hash(str(seed ^ index)))
            for i in range(n):
                yield ran.randint(0, uniqueKeys), ran.randint(0, uniqueValues)
        return sc.parallelize(range(numPartitions), numPartitions).flatMap(gen)

    def createKVDataSet(self, sc, dataType, records, uniqueKeys, uniqueValues, keyLength,
                              valueLength, numPartitions, randomSeed,
                              persistenceType, storageLocation="/tmp/spark-perf-kv-data"):
        inputRDD = self.generateIntData(sc, records, uniqueKeys, uniqueValues, numPartitions, randomSeed)
        if dataType == "string":
            inputRDD = inputRDD.map(lambda (k, v): (self.paddedString(k, keyLength),
                                            self.paddedString(v, valueLength)))
        if persistenceType == "memory":
            rdd = inputRDD.persist(pyspark.StorageLevel.MEMORY_ONLY)
        elif persistenceType == "disk":
            rdd = inputRDD.persist(pyspark.StorageLevel.DISK_ONLY)
        elif persistenceType == "hdfs":
            pass
        rdd.count()
        return rdd


class PerfTest:
    def __init__(self, sc):
        self.sc = sc

    def initialize(self, options):
        self.options = options

    def createInputData(self):
        pass

    def run(self):
        raise NotImplementedError


class SchedulerThroughputTest(PerfTest):
    def run(self):
        options = self.options
        rs = []
        for i in range(options.num_trials):
            start = time.time()
            self.sc.parallelize(range(options.num_tasks), options.num_tasks).count()
            rs.append(time.time() - start)
            time.sleep(options.inter_trial_wait)
        return rs


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
                options.persistent_type, options.storage_location)

    def runTest(self, rdd, reduceTasks):
        raise NotImplementedError

    def run(self):
        options = self.options
        used = []
        for i in range(options.num_trials):
            start = time.time()
            self.runTest(self.rdd, options.reduce_tasks)
            end = time.time()
            used.append(end - start)
            time.sleep(options.inter_trial_wait)
        return used


class KVDataTestInt(KVDataTest):
    def __init__(self, sc):
        KVDataTest.__init__(self, sc, "int")

class AggregateByKey(KVDataTest):
    def runTest(self, rdd, reduceTasks):
        rdd.map(lambda (k, v): (k, int(v))).reduceByKey(lambda x, y: x + y, reduceTasks).count()

class AggregateByKeyInt(KVDataTestInt):

    def runTest(self, rdd, reduceTasks):
        rdd.reduceByKey(lambda x, y: x + y, reduceTasks).count()

class AggregateByKeyNaive(KVDataTest):
    def runTest(self, rdd, reduceTasks):
        rdd.map(lambda (k, v): (k, int(v))).groupByKey(reduceTasks).mapValues(sum).count()

class SortByKey(KVDataTest):
    def runTest(self, rdd, reduceTasks):
        rdd.sortByKey(numPartitions=reduceTasks).count()

class SortByKeyInt(KVDataTestInt):
    def runTest(self, rdd, reduceTasks):
        rdd.sortByKey(numPartitions=reduceTasks).count()

class Count(KVDataTest):
    def runTest(self, rdd, _):
        rdd.count()

class CountWithFilter(KVDataTest):
    def runTest(self, rdd, _):
        rdd.filter(lambda (k, v): int(v) % 2).count()

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
    parser.add_option("--random-seed", type="int", default=1)
    parser.add_option("--persistent-type", default="memory")
    parser.add_option("--storage-location", default="/tmp")
    parser.add_option("--hash-records", action="store_true")
    parser.add_option("--wait-for-exit", action="store_true")

    options, cases = parser.parse_args()

    sc = pyspark.SparkContext("local[*]", "TestRunner")
    for name in cases:
        test = globals()[name](sc)
        test.initialize(options)
        test.createInputData()
        ts = test.run()
        print "results:", ",".join("%.3f" % t for t in ts)