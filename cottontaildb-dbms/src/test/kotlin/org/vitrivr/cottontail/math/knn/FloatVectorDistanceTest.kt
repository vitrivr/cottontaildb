package org.vitrivr.cottontail.math.knn

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.SquaredEuclideanDistance
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.generators.FloatVectorValueGenerator
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.math.isApproximatelyTheSame
import org.vitrivr.cottontail.test.TestConstants
import org.vitrivr.cottontail.utilities.VectorUtility
import kotlin.math.abs
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Test cases that test for correctness of some basic distance calculations with [FloatVectorValue].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class FloatVectorDistanceTest : AbstractDistanceTest() {

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("dimensions")
    fun testL1Distance(dimension: Int) {
        val query = FloatVectorValueGenerator.random(dimension, RANDOM)
        val collection = VectorUtility.randomFloatVectorSequence(dimension, TestConstants.TEST_COLLECTION_SIZE, RANDOM)

        var sum1 = 0.0f
        var sum2 = 0.0f
        var sum3 = 0.0f

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        val kernel = ManhattanDistance.FloatVector(query.type as Types.FloatVector)
        collection.forEach {
            time1 += measureTime {
                sum1 += kernel(query, it).value.toFloat()
            }
            time2 += measureTime {
                sum2 += (query - it).abs().sum().value
            }
            sum3 += l1(it.data, query.data)
        }

        println("Calculating L1 distance for collection (s=${TestConstants.TEST_COLLECTION_SIZE}, d=$dimension) took ${time1 / TestConstants.TEST_COLLECTION_SIZE} (optimized) resp. ${time2 / TestConstants.TEST_COLLECTION_SIZE}  per vector on average.")

        if (time1 > time2) {
            LOGGER.warn("Optimized version of L1 is slower than default version!")
        }
        isApproximatelyTheSame(sum3, sum1)
        isApproximatelyTheSame(sum3, sum2)
    }

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("dimensions")
    fun testL2SquaredDistance(dimension: Int) {
        val query = FloatVectorValueGenerator.random(dimension, RANDOM)
        val collection = VectorUtility.randomFloatVectorSequence(dimension, TestConstants.TEST_COLLECTION_SIZE, RANDOM)

        var sum1 = 0.0f
        var sum2 = 0.0f
        var sum3 = 0.0f

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        val kernel = SquaredEuclideanDistance.FloatVector(query.type as Types.FloatVector)
        collection.forEach {
            time1 += measureTime {
                sum1 += kernel(query, it).value.toFloat()
            }
            time2 += measureTime {
                sum2 += (query - it).pow(2).sum().value
            }
            sum3 += l2squared(it.data, query.data)
        }

        println("Calculating L2^2 distance for collection (s=${TestConstants.TEST_COLLECTION_SIZE}, d=$dimension) took ${time1 / TestConstants.TEST_COLLECTION_SIZE} (optimized) resp. ${time2 / TestConstants.TEST_COLLECTION_SIZE}  per vector on average.")

        if (time1 > time2) {
            LOGGER.warn("Optimized version of L2^2 is slower than default version!")
        }
        isApproximatelyTheSame(sum3, sum1)
        isApproximatelyTheSame(sum3, sum2)
    }

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("dimensions")
    fun testL2Distance(dimension: Int) {
        val query = FloatVectorValueGenerator.random(dimension, RANDOM)
        val collection = VectorUtility.randomFloatVectorSequence(dimension, TestConstants.TEST_COLLECTION_SIZE, RANDOM)

        var sum1 = 0.0f
        var sum2 = 0.0f
        var sum3 = 0.0f

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        val kernel = EuclideanDistance.FloatVector(query.type as Types.FloatVector)
        collection.forEach {
            time1 += measureTime {
                sum1 += kernel(query, it).value.toFloat()
            }
            time2 += measureTime {
                sum2 += (query - it).pow(2).sum().sqrt().value
            }
            sum3 += l2(it.data, query.data)
        }

        println("Calculating L2 distance for collection (s=${TestConstants.TEST_COLLECTION_SIZE}, d=$dimension) took ${time1 / TestConstants.TEST_COLLECTION_SIZE} (optimized) resp. ${time2 / TestConstants.TEST_COLLECTION_SIZE} per vector on average.")

        if (time1 > time2) {
            LOGGER.warn("Optimized version of L2 is slower than default version!")
        }
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
        return kotlin.math.sqrt(sum)
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
}