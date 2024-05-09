package org.vitrivr.cottontail.math.knn

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.InnerProductDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.euclidean.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.squaredeuclidean.SquaredEuclideanDistance
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex64VectorValue
import org.vitrivr.cottontail.core.values.generators.Complex64VectorValueGenerator
import org.vitrivr.cottontail.math.absFromFromComplexFieldVector
import org.vitrivr.cottontail.math.arrayFieldVectorFromVectorValue
import org.vitrivr.cottontail.math.conjFromFromComplexFieldVector
import org.vitrivr.cottontail.math.isApproximatelyTheSame
import org.vitrivr.cottontail.test.TestConstants
import org.vitrivr.cottontail.utilities.VectorUtility
import kotlin.math.pow
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Test cases that test for correctness of some basic distance calculations with [Complex64VectorValue].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex64VectorDistanceTest : AbstractDistanceTest() {

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("dimensions")
    fun testL1Distance(dimension: Int) {
        val query = Complex64VectorValueGenerator.random(dimension, RANDOM)
        val queryp = arrayFieldVectorFromVectorValue(query)
        val collection = VectorUtility.randomComplex64VectorSequence(dimension, TestConstants.TEST_COLLECTION_SIZE, RANDOM)

        var sum1 = 0.0
        var sum2 = 0.0
        var sum3 = 0.0

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO
        var time3 = Duration.ZERO

        val kernel = ManhattanDistance.Complex64Vector(query.type as Types.Complex64Vector)
        collection.forEach {
            time1 += measureTime {
                sum1 += kernel(query , it).value
            }
            time2 += measureTime {
                sum2 += (query - it).abs().sum().value
            }
            time3 += measureTime {
                val dataitem = arrayFieldVectorFromVectorValue(it)
                sum3 += absFromFromComplexFieldVector(queryp.subtract(dataitem)).l1Norm
            }
        }

        println("Calculating L1 distance for collection (s=$TestConstants.collectionSize, d=$dimension) took ${time1 / TestConstants.TEST_COLLECTION_SIZE} (optimized) resp. ${time2 / TestConstants.TEST_COLLECTION_SIZE}  per vector on average.")

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
        val query = Complex64VectorValueGenerator.random(dimension, RANDOM)
        val collection = VectorUtility.randomComplex64VectorSequence(dimension, TestConstants.TEST_COLLECTION_SIZE, RANDOM)
        val queryp = arrayFieldVectorFromVectorValue(query)

        var sum1 = 0.0
        var sum2 = 0.0
        var sum3 = 0.0

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        val kernel = SquaredEuclideanDistance.Complex64Vector(query.type as Types.Complex64Vector)
        collection.forEach {
            time1 += measureTime {
                sum1 += kernel(query, it).value
            }
            time2 += measureTime {
                sum2 += (query - it).abs().pow(2).sum().value
            }
            val dataitem = arrayFieldVectorFromVectorValue(it)
            sum3 += absFromFromComplexFieldVector(queryp.subtract(dataitem)).norm.pow(2)
        }

        println("Calculating L2^2 distance for collection (s=${TestConstants.TEST_COLLECTION_SIZE}, d=$dimension) took ${time1 / TestConstants.TEST_COLLECTION_SIZE} (optimized) resp. ${time2 / TestConstants.TEST_COLLECTION_SIZE}  per vector on average.")

        if (time1 > time2) {
            LOGGER.warn("Optimized version of L1 is slower than default version!")
        }
        isApproximatelyTheSame(sum3, sum1)
        isApproximatelyTheSame(sum3, sum2)
    }

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("dimensions")
    fun testL2Distance(dimension: Int) {
        val query = Complex64VectorValueGenerator.random(dimension, RANDOM)
        val queryp = arrayFieldVectorFromVectorValue(query)
        val collection = VectorUtility.randomComplex64VectorSequence(dimension, TestConstants.TEST_COLLECTION_SIZE, RANDOM)

        var sum1 = 0.0
        var sum2 = 0.0
        var sum3 = 0.0

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        val kernel = EuclideanDistance.Complex64Vector(query.type as Types.Complex64Vector)
        collection.forEach {
            time1 += measureTime {
                sum1 += kernel(query, it).value
            }
            time2 += measureTime {
                sum2 += (query - it).abs().pow(2).sum().sqrt().value
            }
            val dataitem = arrayFieldVectorFromVectorValue(it)
            sum3 += absFromFromComplexFieldVector(queryp.subtract(dataitem)).norm
        }

        println("Calculating L2 distance for collection (s=${TestConstants.TEST_COLLECTION_SIZE}, d=$dimension) took ${time1 / TestConstants.TEST_COLLECTION_SIZE} (optimized) resp. ${time2 / TestConstants.TEST_COLLECTION_SIZE} per vector on average.")

        if (time1 > time2) {
            LOGGER.warn("Optimized version of L1 is slower than default version!")
        }
        isApproximatelyTheSame(sum3, sum1)
        isApproximatelyTheSame(sum3, sum2)
    }

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("dimensions")
    fun testInnerProduct(dimension: Int) {
        val query = Complex64VectorValueGenerator.random(dimension, RANDOM)
        val queryp = arrayFieldVectorFromVectorValue(query)
        val collection = VectorUtility.randomComplex64VectorSequence(dimension, TestConstants.TEST_COLLECTION_SIZE, RANDOM)

        var sum1 = 0.0
        var sum2 = 0.0

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        val kernel = InnerProductDistance.Complex64Vector(query.type as Types.Complex64Vector)
        collection.forEach {
            val conjDataitem = conjFromFromComplexFieldVector(arrayFieldVectorFromVectorValue(it))
            time1 += measureTime {
                sum1 += 1.0 - queryp.dotProduct(conjDataitem).abs()
            }
            time2 += measureTime {
                sum2 += kernel(query, it).value
            }
        }

        println("Calculating abs of DOT for collection (s=${TestConstants.TEST_COLLECTION_SIZE}, d=$dimension) took ${time1 / TestConstants.TEST_COLLECTION_SIZE} (optimized) resp. ${time2 / TestConstants.TEST_COLLECTION_SIZE} per vector on average.")

        if (time1 > time2) {
            LOGGER.warn("Optimized version of L1 is slower than default version!")
        }
        isApproximatelyTheSame(sum1, sum2)
    }
}