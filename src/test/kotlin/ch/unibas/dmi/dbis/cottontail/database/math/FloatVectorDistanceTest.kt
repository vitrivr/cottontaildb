package ch.unibas.dmi.dbis.cottontail.database.math

import ch.unibas.dmi.dbis.cottontail.math.knn.metrics.FloatVectorDistance
import ch.unibas.dmi.dbis.cottontail.math.knn.metrics.Shape
import ch.unibas.dmi.dbis.cottontail.utilities.VectorUtility
import org.junit.jupiter.api.Assertions.*

import org.junit.jupiter.api.RepeatedTest
import java.util.*

import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

import kotlin.time.Duration


class FloatVectorDistanceTest {

    companion object {
        const val COLLECTION_SIZE = 1_000_000
        const val DELTA_LOW = 0.999
        const val DELTA_HIGH = 1.009
        val RANDOM = Random(System.currentTimeMillis())
    }

    @ExperimentalTime
    @RepeatedTest(3)
    fun testL1Distance() {
        val dimensions = RANDOM.nextInt(2048)
        val query = VectorUtility.randomFloatVector(dimensions)
        val collection = VectorUtility.randomFloatVectorSequence(dimensions, COLLECTION_SIZE)

        var sum1 = 0.0
        var sum2 = 0.0

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        collection.forEach {
            time1 += measureTime {
                sum1 += FloatVectorDistance.L1(it, query)
            }
            time2 += measureTime {
                sum2 += FloatVectorDistance.L1(it, query, Shape.S256)
            }
        }

        println("Calculating L1 distance for collection (s=$COLLECTION_SIZE, d=$dimensions) took ${time1/COLLECTION_SIZE} / ${time2/COLLECTION_SIZE} (vectorized) per vector on average.")

        assertTrue(sum1/sum2 < 1.1)
        assertTrue(sum1/sum2 > DELTA_LOW)
    }

    @ExperimentalTime
    @RepeatedTest(3)
    fun testL2SquaredDistance() {
        val dimensions = RANDOM.nextInt(2048)
        val query = VectorUtility.randomFloatVector(dimensions)
        val collection = VectorUtility.randomFloatVectorSequence(dimensions, COLLECTION_SIZE)

        var sum1 = 0.0
        var sum2 = 0.0

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        collection.forEach {
            time1 += measureTime {
                sum1 += FloatVectorDistance.L2SQUARED(it, query)
            }
            time2 += measureTime {
                sum2 += FloatVectorDistance.L2SQUARED(it, query, Shape.S256)
            }
        }

        println("Calculating L2^2 distance for collection (s=$COLLECTION_SIZE, d=$dimensions) took ${time1/COLLECTION_SIZE}, ${time2/COLLECTION_SIZE} (vectorized) per vector on average.")

        assertTrue(sum1/sum2 < DELTA_HIGH)
        assertTrue(sum1/sum2 > DELTA_LOW)
    }

    @ExperimentalTime
    @RepeatedTest(3)
    fun testL2Distance() {
        val dimensions = RANDOM.nextInt(2048)
        val query = VectorUtility.randomFloatVector(dimensions)
        val collection = VectorUtility.randomFloatVectorSequence(dimensions, COLLECTION_SIZE)

        var sum1 = 0.0
        var sum2 = 0.0

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        collection.forEach {
            time1 += measureTime {
                sum1 += FloatVectorDistance.L2(it, query)
            }
            time2 += measureTime {
                sum2 += FloatVectorDistance.L2(it, query, Shape.S256)
            }
        }

        println("Calculating L2 distance for collection (s=$COLLECTION_SIZE, d=$dimensions) took ${time1/COLLECTION_SIZE}, ${time2/COLLECTION_SIZE} (vectorized) per vector on average.")

        assertTrue(sum1/sum2 < DELTA_HIGH)
        assertTrue(sum1/sum2 > DELTA_LOW)
    }

    @ExperimentalTime
    @RepeatedTest(3)
    fun testCosineDistance() {
        val dimensions = RANDOM.nextInt(2048)
        val query = VectorUtility.randomFloatVector(dimensions)
        val collection = VectorUtility.randomFloatVectorSequence(dimensions, COLLECTION_SIZE)

        var sum1 = 0.0
        var sum2 = 0.0

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        collection.forEach {
            time1 += measureTime {
                sum1 += FloatVectorDistance.COSINE(it, query)
            }
            time2 += measureTime {
                sum2 += FloatVectorDistance.COSINE(it, query, Shape.S256)
            }
        }

        println("Calculating Cosine distance for collection (s=$COLLECTION_SIZE, d=$dimensions) took ${time1/COLLECTION_SIZE}, ${time2/COLLECTION_SIZE} (vectorized) per vector on average.")

        assertTrue(sum1/sum2 < DELTA_HIGH)
        assertTrue(sum1/sum2 > DELTA_LOW)
    }
}