"""
Data generation for MLlib spark-perf tests
(+ data loading in the future)
"""

import numpy

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

class FeaturesGenerator:
    """
    Generator for feature vectors for prediction algorithms.
    TODO: Generalize to categorical features later.
    """

    @staticmethod
    def generateContinuousData(sc, numExamples, numFeatures, numPartitions, seed):
        n = numExamples / numPartitions
        def gen(index):
            rng = numpy.random.RandomState(hash(str(seed ^ index)))
            for i in range(n):
                yield Vectors.dense(rng.rand(numFeatures))
        return sc.parallelize(range(numPartitions), numPartitions).flatMap(gen)


class LabeledDataGenerator:
    """
    Data generator for prediction problems
    """

    @staticmethod
    def generateGLMData(sc, numExamples, numFeatures, numPartitions, seed, labelType):
        """
        :param labelType: 0 = unbounded real-valued labels.  2 = binary 0/1 labels
        :param perNegative: Fraction of example to be negative.  Ignore if not using binary labels.
        """
        assert labelType == 0 or labelType == 2, \
          "LabeledDataGenerator.generateGLMData given invalid labelType: %r" % labelType
        rng = numpy.random.RandomState(seed)
        weights = rng.rand(numFeatures)
        featuresRDD = FeaturesGenerator.generateContinuousData(sc, numExamples, numFeatures, numPartitions, seed)
        def makeLP(features):
            label = features.dot(weights)
            if labelType == 2:
                label = 1 if label > 0.0 else 0
            return LabeledPoint(label, features)
        return featuresRDD.map(makeLP)
