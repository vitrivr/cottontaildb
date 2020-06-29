package org.vitrivr.cottontail.math.knn

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.vitrivr.cottontail.math.basics.absFromFromComplexFieldVector
import org.vitrivr.cottontail.math.basics.arrayFieldVectorFromVectorValue
import org.vitrivr.cottontail.math.basics.conjFromFromComplexFieldVector
import org.vitrivr.cottontail.math.knn.metrics.AbsoluteInnerProductDistance
import org.vitrivr.cottontail.math.knn.metrics.EuclidianDistance
import org.vitrivr.cottontail.math.knn.metrics.ManhattanDistance
import org.vitrivr.cottontail.math.knn.metrics.RealInnerProductDistance
import org.vitrivr.cottontail.model.values.Complex64VectorValue
import org.vitrivr.cottontail.utilities.VectorUtility
import java.util.*
import kotlin.math.pow
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Test cases that test for correctness of some basic distance calculations with [Complex64VectorValue].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class Complex64VectorDistanceTest {

    companion object {
        const val COLLECTION_SIZE = 1_000_000
        const val DELTA = 1e-10
        val RANDOM = SplittableRandom()
    }

    @ExperimentalTime
    @ParameterizedTest
    @ValueSource(ints = [32, 64, 128, 256, 512, 1024])
    fun testL1Distance(dimension: Int) {
        val query = Complex64VectorValue.random(dimension, RANDOM)
        val queryp = arrayFieldVectorFromVectorValue(query)
        val collection = VectorUtility.randomComplex64VectorSequence(dimension, COLLECTION_SIZE, RANDOM)

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
            val dataitem = arrayFieldVectorFromVectorValue(it)
            sum3 += absFromFromComplexFieldVector(queryp.subtract(dataitem)).l1Norm
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
        val query = Complex64VectorValue.random(dimension, RANDOM)
        val collection = VectorUtility.randomComplex64VectorSequence(dimension, COLLECTION_SIZE, RANDOM)
        val queryp = arrayFieldVectorFromVectorValue(query)

        var sum1 = 0.0
        var sum2 = 0.0
        var sum3 = 0.0

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        collection.forEach {
            time1 += measureTime {
                sum1 += (it.l2sq(query)).value
            }
            time2 += measureTime {
                sum2 += (query-it).abs().pow(2).sum().value
            }
            val dataitem = arrayFieldVectorFromVectorValue(it)
            sum3 += absFromFromComplexFieldVector(queryp.subtract(dataitem)).norm.pow(2)
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
        val query = Complex64VectorValue.random(dimension, RANDOM)
        val queryp = arrayFieldVectorFromVectorValue(query)
        val collection = VectorUtility.randomComplex64VectorSequence(dimension, COLLECTION_SIZE, RANDOM)

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
                sum2 += (query-it).abs().pow(2).sum().sqrt().value
            }
            val dataitem = arrayFieldVectorFromVectorValue(it)
            sum3 += absFromFromComplexFieldVector(queryp.subtract(dataitem)).norm
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

    @ExperimentalTime
    @ParameterizedTest
    @ValueSource(ints = [32, 64, 128, 256, 512, 1024])
    fun testIPDistance(dimension: Int) {
        val query = Complex64VectorValue.random(dimension, RANDOM)
        val queryp = arrayFieldVectorFromVectorValue(query)
        val collection = VectorUtility.randomComplex64VectorSequence(dimension, COLLECTION_SIZE, RANDOM)

        var sum1 = 0.0
        var sum2 = 0.0

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        collection.forEach {
            val dataitem = arrayFieldVectorFromVectorValue(it)
            time1 += measureTime {
                sum1 += AbsoluteInnerProductDistance(query, it).value
            }
            time2 += measureTime {
                sum2 += 1.0 - queryp.dotProduct(conjFromFromComplexFieldVector(dataitem)).abs()
            }
//            sum3 += queryp.dotProduct(dataitem).abs()
        }
        val sum3 = sum2

        println("Calculating abs of DOT for collection (s=$COLLECTION_SIZE, d=$dimension) took ${time1 / COLLECTION_SIZE} (optimized) resp. ${time2 / COLLECTION_SIZE} per vector on average.")

        assertTrue(time1 < time2, "Optimized version of dot is slower than default version!")
        assertEquals(sum3 , sum1, "dot for optimized version does not equal expected value.")
        assertEquals(sum3 , sum2,"dot for default version does not equal expected value.")
        assertTrue(sum1 / sum3 < 1.0 + DELTA, "Deviation for optimized version detected. Expected: $sum3, Received: $sum1")
        assertTrue(sum1 / sum3 > 1.0 - DELTA, "Deviation for optimized version detected. Expected: $sum3, Received: $sum1")
        assertTrue(sum2 / sum3 < 1.0 + DELTA, "Deviation for manual version detected. Expected: $sum3, Received: $sum2")
        assertTrue(sum2 / sum3 > 1.0 - DELTA, "Deviation for manual version detected. Expected: $sum3, Received: $sum2")
    }

    @ExperimentalTime
    @ParameterizedTest
    @ValueSource(ints = [32, 64, 128, 256, 512, 1024])
    fun testIPRealDistance(dimension: Int) {
        val query = Complex64VectorValue.random(dimension, RANDOM)
        val queryp = arrayFieldVectorFromVectorValue(query)
        val collection = VectorUtility.randomComplex64VectorSequence(dimension, COLLECTION_SIZE, RANDOM)

        var sum1 = 0.0
        var sum2 = 0.0
        var sum3 = 0.0

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        collection.forEach {
            val conjDataitem = conjFromFromComplexFieldVector(arrayFieldVectorFromVectorValue(it))
            time1 += measureTime {
                sum1 += 1.0 - query.dotRealPart(it).value
            }
            time2 += measureTime {
                sum2 += RealInnerProductDistance(query, it).value
            }
            sum3 += 1.0 - queryp.dotProduct(conjDataitem).real
        }

        println("Calculating abs of DOT for collection (s=$COLLECTION_SIZE, d=$dimension) took ${time1 / COLLECTION_SIZE} (optimized) resp. ${time2 / COLLECTION_SIZE} per vector on average.")

        assertTrue(time1 < time2, "Optimized version of dotreal is slower than default version!")
        assertEquals(sum3 , sum1, "dotreal for optimized version does not equal expected value.")
        assertEquals(sum3 , sum2,"dotreal for default version does not equal expected value.")
        assertTrue(sum1 / sum3 < 1.0 + DELTA, "Deviation for optimized version detected. Expected: $sum3, Received: $sum1")
        assertTrue(sum1 / sum3 > 1.0 - DELTA, "Deviation for optimized version detected. Expected: $sum3, Received: $sum1")
        assertTrue(sum2 / sum3 < 1.0 + DELTA, "Deviation for manual version detected. Expected: $sum3, Received: $sum2")
        assertTrue(sum2 / sum3 > 1.0 - DELTA, "Deviation for manual version detected. Expected: $sum3, Received: $sum2")
    }
}