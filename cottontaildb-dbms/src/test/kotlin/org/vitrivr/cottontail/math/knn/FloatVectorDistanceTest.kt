package org.vitrivr.cottontail.math.knn

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.functions.math.distance.binary.*
import org.vitrivr.cottontail.math.isApproximatelyTheSame
import org.vitrivr.cottontail.utilities.VectorUtility
import kotlin.math.abs
import kotlin.math.pow
import kotlin.math.sqrt
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Test cases that test for correctness of some basic distance calculations with [FloatVectorValue].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FloatVectorDistanceTest : AbstractDistanceTest() {

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("dimensions")
    fun testL1Distance(dimension: Int) {
        val query = FloatVectorValue.random(dimension, RANDOM)
        val collection = VectorUtility.randomFloatVectorSequence(dimension, TestConstants.collectionSize, RANDOM)

        var sum1 = 0.0f
        var sum2 = 0.0f
        var sum3 = 0.0f

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO
        var time3= Duration.ZERO

        val kernel = ManhattanDistance.FloatVectorVectorized(query.type as Types.FloatVector)
        collection.forEach {
            time1 += measureTime {
                sum1 += kernel(query, it).value.toFloat()
            }
            time2 += measureTime {
                sum2 += (query - it).abs().sum().value
            }
            time3 += measureTime {
                sum3 += l1(it.data, query.data)
            }
        }

        println("Calculating L1 distance for collection (s=${TestConstants.collectionSize}, d=$dimension) took ${time1 / TestConstants.collectionSize} (optimized) resp. ${time2 / TestConstants.collectionSize}  per vector on average.")

        if (time1 > time2) {
            LOGGER.warn("Optimized version of L1 is slower than default version!")
        }
        println("Optimized: $time1")
        println("Standard: $time2")
        println("Test method: $time3")

        isApproximatelyTheSame(sum3, sum1)
        isApproximatelyTheSame(sum3, sum2)
    }

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("dimensions")
    fun testL2SquaredDistance(dimension: Int) {
        val query = FloatVectorValue.random(dimension, RANDOM)
        val collection = VectorUtility.randomFloatVectorSequence(dimension, TestConstants.collectionSize, RANDOM)

        var sum1 = 0.0f
        var sum2 = 0.0f
        var sum3 = 0.0f

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO
        var time3 = Duration.ZERO

        val kernel = SquaredEuclideanDistance.FloatVector(query.type as Types.FloatVector)
        collection.forEach {
            time1 += measureTime {
                sum1 += kernel(query, it).value.toFloat()
            }
            time2 += measureTime {
                sum2 += (query - it).pow(2).sum().value
            }
            time3 += measureTime {
                sum3 += l2squared(it.data, query.data)
            }
        }

        println("Calculating L2^2 distance for collection (s=${TestConstants.collectionSize}, d=$dimension) took ${time1 / TestConstants.collectionSize} (optimized) resp. ${time2 / TestConstants.collectionSize}  per vector on average.")

        if (time1 > time2) {
            LOGGER.warn("Optimized version of L2^2 is slower than default version!")
        }
        println("Optimized: $time1")
        println("Standard: $time2")
        println("Test method: $time3")

        isApproximatelyTheSame(sum3, sum1)
        isApproximatelyTheSame(sum3, sum2)
    }

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("dimensions")
    fun testL2Distance(dimension: Int) {
        val query = FloatVectorValue.random(dimension, RANDOM)
        val collection = VectorUtility.randomFloatVectorSequence(dimension, TestConstants.collectionSize, RANDOM)

        var sum1 = 0.0f
        var sum2 = 0.0f
        var sum3 = 0.0f

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO
        var time3 = Duration.ZERO

        val kernel = EuclideanDistance.FloatVector(query.type as Types.FloatVector)
        collection.forEach {
            time1 += measureTime {
                sum1 += kernel(query, it).value.toFloat()
            }
            time2 += measureTime {
                sum2 += (query - it).pow(2).sum().sqrt().value
            }
            time3 += measureTime {
                sum3 += l2(it.data, query.data)
            }
        }

        println("Calculating L2 distance for collection (s=${TestConstants.collectionSize}, d=$dimension) took ${time1 / TestConstants.collectionSize} (optimized) resp. ${time2 / TestConstants.collectionSize} per vector on average.")

        if (time1 > time2) {
            LOGGER.warn("Optimized version of L2 is slower than default version!")
        }
        println("Optimized: $time1")
        println("Standard: $time2")
        println("Test method: $time3")

        isApproximatelyTheSame(sum3, sum1)
        isApproximatelyTheSame(sum3, sum2)
    }

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("dimensions")
    fun testCosineDistance(dimension: Int) {
        val query = FloatVectorValue.random(dimension, RANDOM)
        val collection = VectorUtility.randomFloatVectorSequence(dimension, TestConstants.collectionSize, RANDOM)

        var sum1 = 0.0f
        var sum2 = 0.0f
        var sum3 = 0.0f

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO
        var time3 = Duration.ZERO

        val kernel = CosineDistance.FloatVector(query.type as Types.FloatVector)
        collection.forEach {
            time1 += measureTime {
                sum1 += kernel(query, it).value.toFloat()
            }
            time2 += measureTime {
                var dotp = 0.0
                var normq = 0.0
                var normv = 0.0
                for (i in 0 until dimension) {
                    dotp += (query.data[i] * it.data[i])
                    normq += query.data[i].pow(2)
                    normv += it.data[i].pow(2)
                }

                sum2 += (dotp / (sqrt(normq) * sqrt(normv))).toFloat()
            }
            time3 += measureTime {
                sum3 += cosine(query.data, it.data)
            }
        }

        println("Calculating L2 distance for collection (s=${TestConstants.collectionSize}, d=$dimension) took ${time1 / TestConstants.collectionSize} (optimized) resp. ${time2 / TestConstants.collectionSize} per vector on average.")

        if (time1 > time2) {
            LOGGER.warn("Optimized version of L2 is slower than default version!")
        }
        println("Optimized: $time1")
        println("Standard: $time2")
        println("Test method: $time3")

        isApproximatelyTheSame(sum2, sum1)
        isApproximatelyTheSame(sum3, sum1)
    }

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("dimensions")
    fun testChisquaredDistance(dimension: Int) {
        val query = FloatVectorValue.random(dimension, RANDOM)
        val collection = VectorUtility.randomFloatVectorSequence(dimension, TestConstants.collectionSize, RANDOM)

        var sum1 = 0.0f
        var sum2 = 0.0f
        var sum3 = 0.0f

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO
        var time3 = Duration.ZERO

        val kernel = ChisquaredDistance.FloatVector(query.type as Types.FloatVector)
        collection.forEach {
            time1 += measureTime {
                sum1 += kernel(query, it).value.toFloat()
            }
            time2 += measureTime {
                sum2 += (query - it).pow(2).div((query + it)).sum().value
            }
            time3 += measureTime {
                sum3 += chisquared(it.data, query.data)
            }
        }

        println("Calculating L2 distance for collection (s=${TestConstants.collectionSize}, d=$dimension) took ${time1 / TestConstants.collectionSize} (optimized) resp. ${time2 / TestConstants.collectionSize} per vector on average.")

        if (time1 > time2) {
            LOGGER.warn("Optimized version of L2 is slower than default version!")
        }
        println("Optimized: $time1")
        println("Standard: $time2")
        println("Test method: $time3")

        isApproximatelyTheSame(sum3, sum1)
        isApproximatelyTheSame(sum3, sum2)
    }

    /**
     * Calculates the L<sub>1</sub> (sum of abs) distance between two points.
     *
     * @param p1 the first point
     * @param p2 the second point
     * @return the L<sub>1</sub> distance between the two points
     */
    private fun l1(p1: FloatArray, p2: FloatArray): Float {
        require(p1.size == p2.size) { "Dimension mismatch!" }
        var sum = 0.0f
        for (i in p1.indices) {
            sum += abs(p1[i] - p2[i])
        }
        return sum
    }

    /**
     * Calculates the L<sub>2</sub> (Euclidean) distance between two points.
     *
     * @param p1 the first point
     * @param p2 the second point
     * @return the L<sub>2</sub> distance between the two points
     */
    private fun l2(p1: FloatArray, p2: FloatArray): Float {
        require(p1.size == p2.size) { "Dimension mismatch!" }
        var sum = 0.0f
        for (i in p1.indices) {
            val dp = p1[i] - p2[i]
            sum += dp * dp
        }
        return sqrt(sum)
    }

    /**
     * Calculates the L<sub>2</sub> (Euclidean) distance between two points.
     *
     * @param p1 the first point
     * @param p2 the second point
     * @return the L<sub>2</sub> distance between the two points
     */
    private fun l2squared(p1: FloatArray, p2: FloatArray): Float {
        require(p1.size == p2.size) { "Dimension mismatch!" }
        var sum = 0.0f
        for (i in p1.indices) {
            val dp = p1[i] - p2[i]
            sum += dp * dp
        }
        return sum
    }

    /**
     * Calculates the Cosine distance between two points.
     *
     * @param p1 the first point
     * @param p2 the second point
     * @return the cosine distance between the two points
     */
    private fun cosine(p1: FloatArray, p2: FloatArray): Float {
        require(p1.size == p2.size) { "Dimension mismatch!" }
        var dotp = 0.0f
        var normq = 0.0f
        var normv = 0.0f
        for (i in p1.indices) {
            dotp += (p1[i] * p2[i])
            normq += p1[i].pow(2)
            normv += p2[i].pow(2)
        }
        return dotp / (sqrt(normq) * sqrt(normv))
    }

    /**
     * Calculates the Chisquared distance between two points.
     *
     * @param p1 the first point
     * @param p2 the second point
     * @return the chisquared distance between the two points
     */
    private fun chisquared(p1: FloatArray, p2: FloatArray):Float {
        require(p1.size == p2.size) { "Dimension mismatch!" }
        var sum = 0.0f
        for (i in p1.indices) {
            val sub = p1[i] - p2[i]
            val add = p1[i] + p2[i]
            sum += (sub * sub) / (add)
        }
        return sum
    }
}