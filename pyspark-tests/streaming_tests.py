import time
import random
import os
import re

from py4j.java_gateway import java_import

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext, DStream


def createInputStream(ssc, streamIndex, options):

    partitions = int(options.batch_duration * 1000 / 200)  # one block in 200ms
    records = int(options.records_per_second * options.batch_duration / partitions)

    rdd = ssc.sparkContext.parallelize(range(partitions), partitions)
    stream = ssc.queueStream([], default=rdd)
    key_fmt = '%%0%dd' % options.key_length
    value_fmt = '%%0%dd' % options.value_length
    def generatePartition(i, time):
        # Use per-stream, per-time and per-partition seeds to avoid having identical data
        seed = hash((streamIndex, i, time))
        rand = random.Random(seed)
        for i in xrange(records):
            yield (key_fmt % rand.randint(1, options.unique_keys),
                   value_fmt % rand.randint(1, options.unique_values))

    def func(time, rdd):
        return rdd.flatMap(lambda x: generatePartition(x, time))

    return stream.transform(func)


class PerfTest:
    def __init__(self, ssc, options):
        self.ssc = ssc
        self.options = options

    def run(self):
        raise NotImplementedError


class KVDataTest(PerfTest):
    def run(self):
        input = self.setupInputStreams(self.options.num_streams)
        output = self.setupOutputStream(input)
        output.count().pprint()
        sc = self.ssc.sparkContext
        java_import(sc._jvm, "org.apache.spark.streaming.scheduler.StatsReportListener")
        numBatches = int(self.options.total_duration / self.options.batch_duration)
        listener = sc._jvm.StatsReportListener(numBatches)
        self.ssc._jssc.addStreamingListener(listener)
        self.ssc.start()
        startTime = time.time()
        time.sleep(self.options.total_duration)
        self.ssc.stop(False, True)
        return self.processResults(listener)

    def processResults(self, listener):
        # avoid a number of py4j call by serialize infos then parse it
        s = listener.batchInfos().toString()
        s = re.sub(r"Some\((\d+)\)", r"\1", s)
        s = re.sub(r" ms,Map\(.*?\)", '', s)
        infos = [map(long, info.split(","))
                 for info in re.findall(r"BatchInfo\((.*?)\)", s)]
        if len(infos) > 20:
            infos = infos[10:]  # ignore first 10
        delays = sorted([(end - processing) / 1000.0
                         for (batch, submit, processing, end) in infos])
        size = len(delays)
        s = sum(delays)
        avg = s / size
        med = delays[size/2]
        stdev = (sum([(d - avg) ** 2 for d in delays]) / size) ** 0.5
        return "count: %d, avg: %.2f s, stdev: %.2f s, min: %.2f s, 50%%: %.2f s, 95%%: %.2f s, max: %.2f s" % (
            size, avg, stdev, delays[0], med, delays[int(.95*size)], delays[-1]
        )


    def setupInputStreams(self, numStreams):
        inputStreams = [createInputStream(self.ssc, i, self.options)
                        for i in range(numStreams)]
        return self.ssc.union(*inputStreams)

    def setupOutputStream(self, inputStream):
        raise NotImplementedError


class StateByKeyTest(KVDataTest):
    def setupOutputStream(self, inputStream):

        def updateFunc(values, state):
            return sum(values, state or 0)

        return inputStream.mapValues(int).updateStateByKey(updateFunc, self.options.reduce_tasks)


class WindowKVDataTest(KVDataTest):
    pass


class ReduceByKeyAndWindowTest(WindowKVDataTest):
    def setupOutputStream(self, inputStream):
        return inputStream.reduceByKeyAndWindow(lambda x, y: x + y, None,
                                                self.options.window_duration,
                                                self.options.batch_duration,
                                                self.options.reduce_tasks)


class GroupByKeyAndWindowTest(WindowKVDataTest):
    def setupOutputStream(self, inputStream):
        return inputStream.groupByKeyAndWindow(self.options.window_duration,
                                               self.options.batch_duration,
                                               self.options.reduce_tasks)


if __name__ == "__main__":
    import optparse
    parser = optparse.OptionParser(usage="Usage: %prog [options] test_names")
    parser.add_option("--reduce-tasks", type="int", default=2)
    parser.add_option("--inter-trial-wait", type="int", default=0)
    parser.add_option("--unique-keys", type="int", default=1024)
    parser.add_option("--key-length", type="int", default=10)
    parser.add_option("--unique-values", type="int", default=102400)
    parser.add_option("--value-length", type="int", default=20)
    # parser.add_option("--num-partitions", type="int", default=10)
    parser.add_option("--random-seed", type="int", default=1)
    parser.add_option("--persistent-type", default="memory")
    parser.add_option("--checkpoint-location", default="/tmp")
    # parser.add_option("--hash-records", action="store_true")
    # parser.add_option("--wait-for-exit", action="store_true")

    parser.add_option("--num-streams", type="int", default=2)
    parser.add_option("--records-per-second", type="int", default=1024)
    parser.add_option("--batch-duration", type="float", default=1.0)
    parser.add_option("--total-duration", type="float", default=60.0)
    parser.add_option("--window-duration", type="float", default=2.0)

    options, cases = parser.parse_args()

    sc = SparkContext(appName="PythonStreamingTestRunner")
    ssc = StreamingContext(sc, options.batch_duration)
    ssc.checkpoint(os.path.join(options.checkpoint_location, "checkpoint"))
    for name in cases:
        print "Running", name
        test = globals()[name](ssc, options)
        print test.run()
        time.sleep(options.inter_trial_wait)