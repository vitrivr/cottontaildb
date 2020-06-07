package org.vitrivr.cottontail.math.knn

import org.apache.commons.math3.util.MathArrays
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.vitrivr.cottontail.math.knn.metrics.EuclidianDistance
import org.vitrivr.cottontail.math.knn.metrics.ManhattanDistance
import org.vitrivr.cottontail.math.knn.metrics.SquaredEuclidianDistance
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.utilities.VectorUtility
import java.util.*
import kotlin.math.pow
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Test cases that test for correctness of some basic distance calculations with [DoubleVectorValue].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class DoubleVectorDistanceTest {

    companion object {
        const val COLLECTION_SIZE = 1_000_000
        const val DELTA = 1e-10
        val RANDOM = SplittableRandom()
    }

    @ExperimentalTime
    @ParameterizedTest
    @ValueSource(ints = [32, 64, 128, 256, 512, 1024])
    fun testL1Distance(dimension: Int) {
        val query = DoubleVectorValue.random(dimension, RANDOM)
        val collection = VectorUtility.randomDoubleVectorSequence(dimension, COLLECTION_SIZE, RANDOM)

        var sum1 = 0.0
        var sum2 = 0.0
        var sum3 = 0.0

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        collection.forEach {
            time1 += measureTime {
                sum1 += ManhattanDistance(it, query).value
            }
            time2 += measureTime {
                sum2 += (query-it).abs().sum().value
            }
            sum3 += MathArrays.distance1(it.data, query.data)
        }

        println("Calculating L1 distance for collection (s=$COLLECTION_SIZE, d=$dimension) took ${time1 / COLLECTION_SIZE} (optimized) resp. ${time2 / COLLECTION_SIZE}  per vector on average.")

        assertTrue(time1 < time2, "Optimized version of L1 is slower than default version!")
        assertTrue(sum1 / sum3 < 1.0 + DELTA, "Deviation for optimized version detected. Expected: $sum3, Received: $sum1")
        assertTrue(sum1 / sum3 > 1.0 - DELTA, "Deviation for optimized version detected. Expected: $sum3, Received: $sum1")
        assertTrue(sum2 / sum3 < 1.0 + DELTA, "Deviation for manual version detected. Expected: $sum3, Received: $sum2")
        assertTrue(sum2 / sum3 > 1.0 - DELTA, "Deviation for manual version detected. Expected: $sum3, Received: $sum2")
    }

    @ExperimentalTime
    @ParameterizedTest
    @ValueSource(ints = [32, 64, 128, 256, 512, 1024])
    fun testL2SquaredDistance(dimension: Int) {
        val query = DoubleVectorValue.random(dimension, RANDOM)
        val collection = VectorUtility.randomDoubleVectorSequence(dimension, COLLECTION_SIZE, RANDOM)

        var sum1 = 0.0
        var sum2 = 0.0
        var sum3 = 0.0

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        collection.forEach {
            time1 += measureTime {
                sum1 += SquaredEuclidianDistance(it, query).value
            }
            time2 += measureTime {
                sum2 += (query-it).pow(2).sum().value
            }
            sum3 += MathArrays.distance(it.data, query.data).pow(2)
        }

        println("Calculating L2^2 distance for collection (s=$COLLECTION_SIZE, d=$dimension) took ${time1 / COLLECTION_SIZE} (optimized) resp. ${time2 / COLLECTION_SIZE}  per vector on average.")

        assertTrue(time1 < time2, "Optimized version of L2^2 is slower than default version!")
        assertEquals(sum3 , sum1, "L2^2 for optimized version does no equal expected value.")
        assertEquals(sum3 , sum2, "L2^2 for default version does no equal expected value.")
        assertTrue(sum1 / sum3 < 1.0 + DELTA, "Deviation for optimized version detected. Expected: $sum3, Received: $sum1")
        assertTrue(sum1 / sum3 > 1.0 - DELTA, "Deviation for optimized version detected. Expected: $sum3, Received: $sum1")
        assertTrue(sum2 / sum3 < 1.0 + DELTA, "Deviation for manual version detected. Expected: $sum3, Received: $sum2")
        assertTrue(sum2 / sum3 > 1.0 - DELTA, "Deviation for manual version detected. Expected: $sum3, Received: $sum2")
    }

    @ExperimentalTime
    @ParameterizedTest
    @ValueSource(ints = [32, 64, 128, 256, 512, 1024])
    fun testL2Distance(dimension: Int) {
        val query = DoubleVectorValue.random(dimension, RANDOM)
        val collection = VectorUtility.randomDoubleVectorSequence(dimension, COLLECTION_SIZE, RANDOM)

        var sum1 = 0.0
        var sum2 = 0.0
        var sum3 = 0.0

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        collection.forEach {
            time1 += measureTime {
                sum1 += EuclidianDistance(it, query).value
            }
            time2 += measureTime {
                sum2 += (query-it).pow(2).sum().sqrt().value
            }
            sum3 += MathArrays.distance(it.data, query.data)
        }

        println("Calculating L2 distance for collection (s=$COLLECTION_SIZE, d=$dimension) took ${time1 / COLLECTION_SIZE} (optimized) resp. ${time2 / COLLECTION_SIZE} per vector on average.")

        assertTrue(time1 < time2, "Optimized version of L2 is slower than default version!")
        assertEquals(sum3 , sum1, "L2 for optimized version does not equal expected value.")
        assertEquals(sum3 , sum2,"L2 for default version does not equal expected value.")
        assertTrue(sum1 / sum3 < 1.0 + DELTA, "Deviation for optimized version detected. Expected: $sum3, Received: $sum1")
        assertTrue(sum1 / sum3 > 1.0 - DELTA, "Deviation for optimized version detected. Expected: $sum3, Received: $sum1")
        assertTrue(sum2 / sum3 < 1.0 + DELTA, "Deviation for manual version detected. Expected: $sum3, Received: $sum2")
        assertTrue(sum2 / sum3 > 1.0 - DELTA, "Deviation for manual version detected. Expected: $sum3, Received: $sum2")
    }
}