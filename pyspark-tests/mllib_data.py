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
        def mapPart(idx, part):
            rng = numpy.random.RandomState(hash(str(seed ^ idx)) & 0xffffffff)
            for i in part:
                yield Vectors.dense(rng.rand(numFeatures))
        return sc.parallelize(xrange(numExamples), numPartitions).mapPartitionsWithIndex(mapPart)


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
        rng = numpy.random.RandomState(hash(str(seed ^ -1)) & 0xffffffff)
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
        def mapPart(idx, part):
            rng = numpy.random.RandomState(hash(str(seed ^ idx)) & 0xffffffff)
            for i in part:
                user = rng.randint(numUsers)
                prod = rng.randint(numProducts)
                rating = float(rng.randint(2)) if implicitPrefs else rng.rand() * 5
                yield (user, prod, rating)
        return sc.parallelize(xrange(numRatings), numPartitions).mapPartitionsWithIndex(mapPart)
