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
        :return: RDD[LabeledPoint]
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


class RatingGenerator:
    """
    Data generator for recommendation problems
    """

    @staticmethod
    def generateRatingData(sc, numUsers, numProducts, numRatings, implicitPrefs, numPartitions,
                           seed):
        """
        :return: RDD[rating] where each rating is a tuple [userID, productID, rating value]
        """
        assert numUsers > 1, \
            "RatingGenerator.generateRatingData given invalid numUsers = %d" % numUsers
        assert numProducts > 1, \
            "RatingGenerator.generateRatingData given invalid numProducts = %d" % numProducts
        assert numRatings / numUsers <= numProducts, \
            "RatingGenerator.generateRatingData given numRatings=%d too large for numUsers=%d, numProducts=%d" \
            % (numRatings, numUsers, numProducts)
        n = numRatings / numPartitions
        def gen(index):
            rng = numpy.random.RandomState(hash(str(seed ^ index)))
            observed = set()
            for i in range(n):
                pair = (rng.randint(numUsers), rng.randint(numProducts))
                while pair in observed:
                    pair = (rng.randint(numUsers), rng.randint(numProducts))
                observed.add(pair)
                rating = float(rng.randint(2)) if implicitPrefs else rng.rand() * 5
                yield (pair[0], pair[1], rating)
        return sc.parallelize(range(numPartitions), numPartitions).flatMap(gen)
