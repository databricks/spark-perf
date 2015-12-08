import json
import numpy
import time

import pyspark
from pyspark.ml.classification import LogisticRegression as MLLogisticRegression
from pyspark.ml.regression import LinearRegression as MLLinearRegression
from pyspark.mllib.classification import *
from pyspark.mllib.clustering import *
from pyspark.mllib.regression import *
from pyspark.mllib.recommendation import *
from pyspark.mllib.stat import *

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
        :return: List of [trainingTime, testTime, trainingMetric, testMetric] tuples,
                 or list of [time] tuples
        """
        raise NotImplementedError


class NonPredictionTest(PerfTest):
    def __init__(self, sc):
        PerfTest.__init__(self, sc)

    def runTest(self):
        raise NotImplementedError

    def run(self):
        """
        :return: List of [time] 1-element tuples
        """
        options = self.options
        results = []
        for i in range(options.num_trials):
            start = time.time()
            self.runTest()
            runtime = time.time() - start
            results.append([runtime])
            time.sleep(options.inter_trial_wait)
        return results


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
        self.trainRDD.cache() # match Scala tests for caching before computing testTime
        self.trainRDD.count()
        results = []
        for i in range(options.num_trials):
            # Train
            start = time.time()
            model = self.train(self.trainRDD)
            trainingTime = time.time() - start
            # Measure test time on training set since it is probably larger.
            start = time.time()
            print 'computing trainingMetric...'
            trainingMetric = self.evaluate(model, self.trainRDD)
            print '  done computing trainingMetric'
            testTime = time.time() - start
            # Test
            print 'computing testMetric...'
            testMetric = self.evaluate(model, self.testRDD)
            print '  done computing testMetric'
            results.append([trainingTime, testTime, trainingMetric, testMetric])
            time.sleep(options.inter_trial_wait)
        return results

    @classmethod
    def _evaluateAccuracy(cls, model, rdd):
        """
        :return:  0/1 classification accuracy as percentage for model on the given data.
        """
        acc = rdd.map(lambda lp: 1.0 if lp.label == model.predict(lp.features) else 0.0).mean()
        return 100.0 * acc

    @classmethod
    def _evaluateRMSE(cls, model, rdd):
        """
        :return:  root mean squared error (RMSE) for model on the given data.
        """
        squaredError =\
            rdd.map(lambda lp: numpy.square(lp.label - model.predict(lp.features))).mean()
        return numpy.sqrt(squaredError)


class GLMTest(PredictionTest):
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


class GLMClassificationTest(GLMTest):
    def __init__(self, sc):
        GLMTest.__init__(self, sc)

    def train(self, rdd):
        """
        :return:  Trained model to be passed to test.
        """
        options = self.options
        if options.reg_type == "elastic-net":  # use spark.ml
            lr = MLLogisticRegression(maxIter=options.num_iterations, regParam=options.reg_param,
                                      elasticNetParam=options.elastic_net_param)
            # TODO: Do not include time for conversion to DataFrame (but this currently matches
            #       the Scala tests)
            df = rdd.toDF()
            lrModel = lr.fit(df)
            numFeatures = len(lrModel.weights)
            numClasses = 2
            return LogisticRegressionModel(lrModel.weights, lrModel.intercept,
                                           numFeatures, numClasses)
        else:
            if options.loss == "logistic":
                if options.optimizer == "sgd":
                    return LogisticRegressionWithSGD.train(data=rdd,
                                                           iterations=options.num_iterations,
                                                           step=options.step_size,
                                                           miniBatchFraction=1.0,
                                                           regParam=options.reg_param,
                                                           regType=options.reg_type)
                elif options.optimizer == "l-bfgs":
                    return LogisticRegressionWithLBFGS.train(data=rdd,
                                                             iterations=options.num_iterations,
                                                             regParam=options.reg_param,
                                                             regType=options.reg_type,
                                                             tolerance=0.0)
                else:
                    raise Exception("GLMClassificationTest cannot run with loss = %s,"
                                    " optimizer = %s" % (options.loss, options.optimizer))
            elif options.loss == "hinge":
                if options.optimizer == "sgd":
                    return SVMWithSGD.train(data=rdd, iterations=options.num_iterations,
                                            step=options.step_size, regParam=options.reg_param,
                                            miniBatchFraction=1.0, regType=options.reg_type)
            else:
                raise Exception("GLMClassificationTest does not recognize loss: %s" % options.loss)

    def evaluate(self, model, rdd):
        return PredictionTest._evaluateAccuracy(model, rdd)


class GLMRegressionTest(GLMTest):
    def __init__(self, sc):
        GLMTest.__init__(self, sc)

    def train(self, rdd):
        """
        This ignores the optimizer parameter since it makes config difficult for Linear Regression.
        :return:  Trained model to be passed to test.
        """
        options = self.options
        if options.loss == "l2":
            if options.reg_type in ["none", "l1", "l2"]:
                return LinearRegressionWithSGD.train(data=rdd,
                                                     iterations=options.num_iterations,
                                                     step=options.step_size,
                                                     miniBatchFraction=1.0,
                                                     regParam=options.reg_param,
                                                     regType=options.reg_type)
            elif options.reg_type == "elastic-net":  # use spark.ml
                lr = MLLinearRegression(maxIter=options.num_iterations, regParam=options.reg_param,
                                        elasticNetParam=options.elastic_net_param)
                # TODO: Do not include time for conversion to DataFrame (but this currently matches
                #       the Scala tests)
                df = rdd.toDF()
                lrModel = lr.fit(df)
                return LinearRegressionModel(lrModel.weights, lrModel.intercept)
            else:
                raise Exception("GLMRegressionTest cannot run with loss = %s, reg_type = %s" \
                                % (options.loss, options.reg_type))
        else:
            raise Exception("GLMRegressionTest does not recognize loss: %s" % options.loss)

    def evaluate(self, model, rdd):
        return PredictionTest._evaluateRMSE(model, rdd)


class NaiveBayesTest(PredictionTest):
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

    def evaluate(self, model, rdd):
        return PredictionTest._evaluateAccuracy(model, rdd)

    def train(self, rdd):
        return NaiveBayes.train(rdd, lambda_=options.nb_lambda)


class KMeansTest(NonPredictionTest):
    def __init__(self, sc):
        NonPredictionTest.__init__(self, sc)

    def createInputData(self):
        options = self.options
        self.data = FeaturesGenerator.generateContinuousData(
            self.sc, options.num_examples, options.num_features,
            options.num_partitions, options.random_seed)

    def runTest(self):
        model = KMeans.train(self.data, k=options.num_centers,
                             maxIterations=options.num_iterations)


class ALSTest(PredictionTest):
    def __init__(self, sc):
        PredictionTest.__init__(self, sc)

    def createInputData(self):
        options = self.options
        numTrain = options.num_ratings
        numTest = int(options.num_ratings * 0.2)
        self.trainRDD = RatingGenerator.generateRatingData(
            self.sc, options.num_users, options.num_products, numTrain,
            options.implicit_prefs, options.num_partitions, options.random_seed)
        self.testRDD = RatingGenerator.generateRatingData(
            self.sc, options.num_users, options.num_products, numTest,
            options.implicit_prefs, options.num_partitions, options.random_seed + 1)

    def evaluate(self, model, rdd):
        """
        :return:  root mean squared error (RMSE) for model on the given ratings.
        """
        implicit_prefs = self.options.implicit_prefs
        predictions = model.predictAll(rdd.map(lambda r: (r[0], r[1])))
        sparkVersion = float(str(sc.version)[:3])
        def mapPrediction(r):
            if sparkVersion <= 1.1:
                (user, product, rating) = (r[0], r[1], r[2])
            else:
                (user, product, rating) = (r.user, r.product, r.rating)
            mappedRating = max(min(rating, 1.0), 0.0) if implicit_prefs else rating
            return ((user, product), mappedRating)
        predictionsAndRatings = \
            predictions.map(mapPrediction).join(rdd.map(lambda r: ((r[0], r[1]), r[2]))).values()
        return numpy.sqrt(predictionsAndRatings.map(lambda ab: numpy.square(ab[0] - ab[1])).mean())

    def train(self, rdd):
        if options.implicit_prefs:
            model = ALS.trainImplicit(rdd, rank=options.rank,
                                      iterations=options.num_iterations,
                                      lambda_=options.reg_param, blocks=options.num_partitions)
        else:
            model = ALS.train(rdd, rank=options.rank,
                              iterations=options.num_iterations,
                              lambda_=options.reg_param, blocks=options.num_partitions)
        return model


class CorrelationTest(NonPredictionTest):
    def __init__(self, sc):
        NonPredictionTest.__init__(self, sc)

    def createInputData(self):
        options = self.options
        self.data = FeaturesGenerator.generateContinuousData(
            self.sc, options.num_rows, options.num_cols,
            options.num_partitions, options.random_seed)


class PearsonCorrelationTest(CorrelationTest):
    def __init__(self, sc):
        CorrelationTest.__init__(self, sc)

    def runTest(self):
        corr = Statistics.corr(self.data, method="pearson")


class SpearmanCorrelationTest(CorrelationTest):
    def __init__(self, sc):
        CorrelationTest.__init__(self, sc)

    def runTest(self):
        corr = Statistics.corr(self.data, method="spearman")


if __name__ == "__main__":
    import optparse
    parser = optparse.OptionParser(usage="Usage: %prog [options] test_names")
    # COMMON_OPTS
    parser.add_option("--num-trials", type="int", default=1)
    parser.add_option("--inter-trial-wait", type="int", default=3)
    # MLLIB_COMMON_OPTS
    parser.add_option("--num-partitions", type="int", default=10)
    parser.add_option("--random-seed", type="int", default=5)
    parser.add_option("--num-iterations", type="int", default=20)
    parser.add_option("--reg-param", type="float", default=0.1)
    parser.add_option("--rank", type="int", default=2)
    # MLLIB_REGRESSION_CLASSIFICATION_TEST_OPTS
    parser.add_option("--num-examples", type="int", default=1024)
    parser.add_option("--num-features", type="int", default=50)
    # MLLIB_GLM_TEST_OPTS
    parser.add_option("--step-size", type="float", default=0.1)
    parser.add_option("--reg-type", type="string", default="none")
    parser.add_option("--loss", type="string", default="L2")
    parser.add_option("--optimizer", type="string", default="sgd")
    parser.add_option("--elastic-net-param", type="float", default=0.0)
    # MLLIB_GLM_REGRESSION_TEST_OPTS
    parser.add_option("--intercept", type="float", default=0.0)
    parser.add_option("--label-noise", type="float", default=0.1)
    # MLLIB_CLASSIFICATION_TEST_OPTS
    parser.add_option("--feature-noise", type="float", default=1.0)
    # NAIVE_BAYES_TEST_OPTS
    parser.add_option("--per-negative", type="float", default=0.3)
    parser.add_option("--nb-lambda", type="float", default=1.0)
    parser.add_option("--model-type", type="string", default="multinomial")
    # MLLIB_DECISION_TREE_TEST_OPTS
    parser.add_option("--label-type", type="int", default=2)
    parser.add_option("--frac-categorical-features", type="float", default=0.5)
    parser.add_option("--frac-binary-features", type="float", default=0.5)
    parser.add_option("--tree-depth", type="int", default=5)
    parser.add_option("--max-bins", type="int", default=32)
    #  (for Spark 1.2+ only:)
    parser.add_option("--ensemble-type", type="string", default="RandomForest")
    parser.add_option("--num-trees", type="int", default=1)
    parser.add_option("--feature-subset-strategy", type="string", default="auto")
    # MLLIB_RECOMMENDATION_TEST_OPTS
    parser.add_option("--num-users", type="int", default=60)
    parser.add_option("--num-products", type="int", default=50)
    parser.add_option("--num-ratings", type="int", default=500)
    parser.add_option("--implicit-prefs", type="int", default=0)
    # MLLIB_CLUSTERING_TEST_OPTS
    parser.add_option("--num-centers", type="int", default=5)
    # MLLIB_LINALG_TEST_OPTS + MLLIB_STATS_TEST_OPTS
    parser.add_option("--num-rows", type="int", default=1000)
    parser.add_option("--num-cols", type="int", default=10)

    options, cases = parser.parse_args()

    sc = pyspark.SparkContext(appName="MLlibTestRunner")
    for name in cases:
        test = globals()[name](sc)
        test.initialize(options)
        test.createInputData()
        javaSystemProperties = sc._jvm.System.getProperties()
        systemProperties = {}
        for k in javaSystemProperties.keys():
            if type(javaSystemProperties[k]) != unicode:
                print "type(javaSystemProperties[k]) != unicode"
                print "\t type(javaSystemProperties[k]) = %r" % type(javaSystemProperties[k])
            systemProperties[k] = javaSystemProperties[k]
        ts = test.run()
        if len(ts) != test.options.num_trials:
            raise Exception("mllib_tests.py FAILED (got %d results instead of %d)" %
                            (len(ts), test.options.num_trials))
        results = []
        if len(ts[0]) == 1:
            # results include: time
            print "Results from each trial:"
            print "trial\ttime"
            for trial in range(test.options.num_trials):
                t = ts[trial]
                print "%d\t%.3f" % (trial, t[0])
                results.append({"time": t[0]})
        else:
            # results include: trainingTime, testTime, trainingMetric, testMetric
            print "Results from each trial:"
            print "trial\ttrainingTime\ttestTime\ttrainingMetric\ttestMetric"
            for trial in range(test.options.num_trials):
                t = ts[trial]
                print "%d\t%.3f\t%.3f\t%.3f\t%.3f" % (trial, t[0], t[1], t[2], t[3])
                results.append({"trainingTime": t[0], "testTime": t[1],
                                "trainingMetric": t[2], "testMetric": t[3]})
        # JSON results
        sparkConfInfo = {} # convert to dict to match Scala JSON
        for (a,b) in sc._conf.getAll():
            sparkConfInfo[a] = b
        jsonResults = json.dumps({"testName": name,
                                  "options": vars(options),
                                  "sparkConf": sparkConfInfo,
                                  "sparkVersion": sc.version,
                                  "systemProperties": systemProperties,
                                  "results": results},
                                 separators=(',', ':'))  # use separators for compact encoding
        print "results: " + jsonResults
