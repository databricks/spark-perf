import time

import pyspark
from pyspark.mllib.classification import *

from mllib_data import *


class PerfTest:
    def __init__(self, sc):
        self.sc = sc

    def initialize(self, options):
        self.options = options

    def createInputData(self):
        raise NotImplementedError

    def run(self):
        """
        :return: List of [trainingTime, testTime, trainingMetric, testMetric] tuples
        """
        raise NotImplementedError

class PredictionTest(PerfTest):
    def __init__(self, sc):
        PerfTest.__init__(self, sc)

    def train(self, rdd):
        """
        :return:  Trained model to be passed to test.
        """
        raise NotImplementedError

    def evaluate(self, model, rdd):
        """
        :return:  Evaluation metric for model on the given data.
        """
        raise NotImplementedError

    def run(self):
        options = self.options
        results = []
        for i in range(options.num_trials):
            # Train
            start = time.time()
            model = self.train(self.trainRDD)
            trainingTime = time.time() - start
            # Measure test time on training set since it is probably larger.
            start = time.time()
            trainingMetric = self.evaluate(model, self.trainRDD)
            testTime = time.time() - start
            # Test
            testMetric = self.evaluate(model, self.testRDD)
            results.append([trainingTime, testTime, trainingMetric, testMetric])
            time.sleep(options.inter_trial_wait)
        return results


class GLMClassificationTest(PredictionTest):
    def __init__(self, sc):
        PredictionTest.__init__(self, sc)

    def createInputData(self):
        options = self.options
        numTrain = options.num_examples
        numTest = int(options.num_examples * 0.2)
        self.trainRDD = LabeledDataGenerator.generateGLMData(
            self.sc, numTrain, options.num_features,
            options.num_partitions, options.random_seed, labelType=2)
        self.testRDD = LabeledDataGenerator.generateGLMData(
            self.sc, numTest, options.num_features,
            options.num_partitions, options.random_seed + 1, labelType=2)

    def train(self, rdd):
        """
        :return:  Trained model to be passed to test.
        """
        options = self.options
        if options.loss == "logistic":
            if options.optimizer == "sgd":
                return LogisticRegressionWithSGD.train(data=rdd,
                                                       iterations=options.num_iterations,
                                                       step=options.step_size,
                                                       miniBatchFraction=1.0,
                                                       regParam=options.reg_param,
                                                       regType=options.reg_type)
            else:
                raise Exception("GLMClassificationTest cannot run with loss = %s, optimizer = %s" \
                                % (options.loss, options.optimizer))
        elif options.loss == "hinge":
            if options.optimizer == "sgd":
                return SVMWithSGD.train(data=rdd, iterations=options.num_iterations,
                                        step=options.step_size, regParam=options.reg_param,
                                        miniBatchFraction=1.0, regType=options.reg_type)
        else:
            raise Exception("GLMClassificationTest does not recognize loss: %s" % options.loss)

    def evaluate(self, model, rdd):
        """
        :return:  0/1 classification accuracy as percentage for model on the given data.
        """
        n = rdd.count()
        acc = rdd.map(lambda lp: 1.0 if lp.label == model.predict(lp.features) else 0.0).sum()
        return 100.0 * (acc / n)


if __name__ == "__main__":
    import optparse
    parser = optparse.OptionParser(usage="Usage: %prog [options] test_names")
    # COMMON_OPTS
    parser.add_option("--num-trials", type="int", default=1)
    parser.add_option("--inter-trial-wait", type="int", default=3)
    # MLLIB_COMMON_OPTS
    parser.add_option("--num-partitions", type="int", default=10)
    parser.add_option("--random-seed", type="int", default=5)
    # MLLIB_REGRESSION_CLASSIFICATION_TEST_OPTS
    parser.add_option("--num-examples", type="int", default=1024)
    parser.add_option("--num-features", type="int", default=50)
    # MLLIB_GLM_TEST_OPTS
    parser.add_option("--num-iterations", type="int", default=20)
    parser.add_option("--step-size", type="float", default=0.1)
    parser.add_option("--reg-type", type="string", default="none")
    parser.add_option("--reg-param", type="float", default=0.1)
    parser.add_option("--loss", type="string", default="L2")
    parser.add_option("--optimizer", type="string", default="sgd")
    # MLLIB_GLM_REGRESSION_TEST_OPTS
    parser.add_option("--intercept", type="float", default=0.0)
    parser.add_option("--epsilon", type="float", default=0.1)
    # MLLIB_CLASSIFICATION_TEST_OPTS
    parser.add_option("--scale-factor", type="float", default=1.0)
    # NAIVE_BAYES_TEST_OPTS
    parser.add_option("--per-negative", type="float", default=0.3)
    parser.add_option("--nb-lambda", type="float", default=1.0)

    options, cases = parser.parse_args()

    sc = pyspark.SparkContext(appName="MLlibTestRunner")
    for name in cases:
        test = globals()[name](sc)
        test.initialize(options)
        test.createInputData()
        ts = test.run()
        print "Results from each trial:"
        print "trial\ttrainTime\ttestTime\ttrainMetric\ttestMetric"
        for trial in range(test.options.num_trials):
            t = ts[trial]
            print "%d\t%.3f\t%.3f\t%.3f\t%.3f" % (trial, t[0], t[1], t[2], t[3])
