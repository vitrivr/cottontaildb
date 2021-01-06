package org.vitrivr.cottontail.database.index.va

import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.vitrivr.cottontail.database.index.va.marks.Marks
import org.vitrivr.cottontail.database.index.va.marks.MarksGenerator
import org.vitrivr.cottontail.math.knn.metrics.AbsoluteInnerProductDistance
import org.vitrivr.cottontail.model.values.Complex64Value
import org.vitrivr.cottontail.model.values.Complex64VectorValue
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.testutils.getComplexVectorsFromFile
import java.util.*
import kotlin.math.pow
import kotlin.reflect.full.createType
import kotlin.reflect.full.declaredMemberProperties
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

internal class VAFunctionsKtTest {
    val random = Random(1234)
    val numVecs = 100
    val numDim = 20
    val marksPerDim = 100
    val realdata = Array(numVecs) {
        DoubleArray(numDim) { random.nextGaussian() }
    }

    val imaginarydata = Array(numVecs) { // imaginary parts
        DoubleArray(numDim) { random.nextGaussian() }
    }
    val realmarks = MarksGenerator.getEquidistantMarks(realdata, IntArray(realdata.first().size) { marksPerDim })
    val imaginarymarks = MarksGenerator.getEquidistantMarks(imaginarydata, IntArray(imaginarydata.first().size) { marksPerDim })

    data class BoundTightness(val lbDistReal: Double,
                              val ubDistReal: Double,
                              val lbDistImag: Double,
                              val ubDistImag: Double,
                              val lbDistAbs: Double,
                              val ubDistAbs: Double
    )

    @Test
    fun boundL2Real() {
        // this is a test implementing what's on p8 top of blott&weber 1997
        for (vector in realdata) {
            /*
            todo: there will be an indexOutOfBounds error if the vector component is not contained in the marks!
                  this can happen if the marks are not generated based on the entire data in the DB, but just sampled
                  from there! We could artificially add the Double.Max_VALUE and MIN_VALUE to the marks to catch this
                  see MarksGenerator
            */
            val query = DoubleArray(vector.size) { random.nextGaussian() }
            val cellsVec = realmarks.getCells(vector)
            val realmarks = realmarks
            val lbl2sq = lowerBoundComponentDifferences(cellsVec, query, realmarks).map { it.pow(2) }.sum()
            val ubl2sq = upperBoundComponentDifferences(cellsVec, query, realmarks).map { it.pow(2) }.sum()
            val l2sq = DoubleVectorValue(vector).l2(DoubleVectorValue(query)).pow(2).value
            println("actual squared l2 norm $l2sq")
            println("ub of squared l2 norm $ubl2sq")
            println("lb of squared l2 norm $lbl2sq")
            assertTrue(l2sq <= ubl2sq, "actual l2 larger than upper bound!!")
            assertTrue(l2sq >= lbl2sq, "actual l2 smaller than lower bound!!")
        }
    }

    @Test
    fun boundL2Complex() {
        (realdata zip imaginarydata).forEach { (vectorReal, vectorImag) ->
            /*
            todo: there will be an indexOutOfBounds error if the vector component is not contained in the marks!
                  this can happen if the marks are not generated based on the entire data in the DB, but just sampled
                  from there! We could artificially add the Double.Max_VALUE and MIN_VALUE to the marks to catch this
                  see MarksGenerator
            */
            val queryReal = DoubleArray(vectorReal.size) { random.nextGaussian() }
            val queryImag = DoubleArray(vectorImag.size) { random.nextGaussian() }
            val cellsVecReal = realmarks.getCells(vectorReal)
            val cellsVecImag = imaginarymarks.getCells(vectorImag)
            val lbl2sqReal = lowerBoundComponentDifferences(cellsVecReal, queryReal, realmarks).map { it.pow(2) }.sum()
            val lbl2sqImag = lowerBoundComponentDifferences(cellsVecImag, queryImag, imaginarymarks).map { it.pow(2) }.sum()
            val lbl2sq = lbl2sqReal + lbl2sqImag
            val ubl2sqReal = upperBoundComponentDifferences(cellsVecReal, queryReal, realmarks).map { it.pow(2) }.sum()
            val ubl2sqImag = upperBoundComponentDifferences(cellsVecImag, queryImag, imaginarymarks).map { it.pow(2) }.sum()
            val ubl2sq = ubl2sqReal + ubl2sqImag
            val l2sq = Complex64VectorValue((vectorReal zip vectorImag).map { (a, b) -> Complex64Value(a, b) }.toTypedArray())
                    .l2(Complex64VectorValue((queryReal zip queryImag).map { (a, b) -> Complex64Value(a, b) }.toTypedArray()))
                    .pow(2).value
            println("actual squared l2 norm $l2sq")
            println("ub of squared l2 norm $ubl2sq")
            println("lb of squared l2 norm $lbl2sq")
            assertTrue(l2sq <= ubl2sq, "actual l2 larger than upper bound!!")
            assertTrue(l2sq >= lbl2sq, "actual l2 smaller than lower bound!!")
        }
    }

    @Test
    fun boundRealDotProduct() {
        realdata.forEach { vector ->
            val query = DoubleArray(vector.size) { random.nextGaussian() }
            val cellsVec = realmarks.getCells(vector)
            val lbDP = lowerBoundComponentProductsSum(cellsVec, query, realmarks)
            val ubDP = upperBoundComponentProductsSum(cellsVec, query, realmarks)
            val DP = DoubleVectorValue(vector).dot(DoubleVectorValue(query))
            println("actual dot product $DP")
            println("lb of DP $lbDP")
            println("ub of DP $ubDP")
            assertTrue(DP >= lbDP, "actual DP smaller than lower bound!")
            assertTrue(DP <= ubDP, "actual DP greater than upper bound!")
        }
    }

    /*
    Tests whether the upper and lower bounds on the real and imaginary
     */
    @Test
    fun boundComplexDotProduct() {
        (realdata zip imaginarydata).map {
            testComplexInnerProductBounds(
                    it.first,
                    it.second,
                    DoubleArray(it.first.size) { random.nextGaussian() },
                    DoubleArray(it.first.size) { random.nextGaussian() },
                    realmarks,
                    imaginarymarks
            )
        }
    }

    private fun testComplexInnerProductBounds(realParts: DoubleArray, imaginaryParts: DoubleArray, queryReal: DoubleArray, queryImag: DoubleArray, marksReal: Marks, marksImag: Marks): BoundTightness {
        val cellsVecReal = marksReal.getCells(realParts)
        val cellsVecImag = marksImag.getCells(imaginaryParts)
        val lbDPReal = lbComplexInnerProductReal(cellsVecReal, queryReal, cellsVecImag, queryImag, marksReal, marksImag)
        val lbDPImag = lbComplexInnerProductImag(cellsVecImag, queryReal, cellsVecReal, queryImag, marksReal, marksImag)
        val ubDPReal = ubComplexInnerProductReal(cellsVecReal, queryReal, cellsVecImag, queryImag, marksReal, marksImag)
        val ubDPImag = ubComplexInnerProductImag(cellsVecImag, queryReal, cellsVecReal, queryImag, marksReal, marksImag)
        val dot = Complex64VectorValue((realParts zip imaginaryParts).map { (a, b) -> Complex64Value(a, b) }.toTypedArray()).dot(Complex64VectorValue((queryReal zip queryImag).map { (a, b) -> Complex64Value(a, b) }.toTypedArray()))
        val DPReal = dot.real.value
        val DPImag = dot.imaginary.value
        println("actual real part of dot product $DPReal")
        println("lb of DPReal $lbDPReal")
        println("ub of DPReal $ubDPReal")
        println("actual imaginary part of dot product $DPImag")
        println("lb of DPImag $lbDPImag")
        println("ub of DPImag $ubDPImag")
        assertTrue(DPReal >= lbDPReal, "actual DPReal smaller than lower bound!")
        assertTrue(DPImag >= lbDPImag, "actual DPImag smaller than lower bound!")
        assertTrue(DPReal <= ubDPReal, "actual DPReal greater than upper bound!")
        assertTrue(DPImag <= ubDPImag, "actual DPImag greater than upper bound!")

        val lbDPabsSq = lbAbsoluteComplexInnerProductSq(lbDPReal, ubDPReal, lbDPImag, ubDPImag)
        val ubDPabsSq = ubAbsoluteComplexInnerProductSq(lbDPReal, ubDPReal, lbDPImag, ubDPImag)
        val dpabsSq = dot.abs().pow(2).value
        println("actual magnitude of dot product $dpabsSq")
        println("lb of dpabs $lbDPabsSq")
        println("ub of dpabs $ubDPabsSq")
        assertTrue(dpabsSq >= lbDPabsSq, "actual DPabs smaller than lower bound!")
        assertTrue(dpabsSq <= ubDPabsSq, "actual DPabs greater than upper bound!")
        return BoundTightness(DPReal - lbDPReal, ubDPReal - DPReal, DPImag - lbDPImag, ubDPImag - DPImag, dpabsSq - lbDPabsSq, ubDPabsSq - dpabsSq)
    }


    @ExperimentalStdlibApi
    @Test
    fun boundComplexDotProductFromFile() {
        val data = getComplexVectorsFromFile("src/test/resources/sampledVectors.csv/", 1, numDim)
        val dataReal = data.map { vec -> vec.map { it.real.value }.toDoubleArray() }.toTypedArray()
        val dataImag = data.map { vec -> vec.map { it.imaginary.value }.toDoubleArray() }.toTypedArray()
        val marksReal = MarksGenerator.getEquidistantMarks(dataReal, IntArray(numDim) { marksPerDim })
        val marksImag = MarksGenerator.getEquidistantMarks(dataImag, IntArray(numDim) { marksPerDim })
        val marksRealNonUniform = MarksGenerator.getNonUniformMarks(dataReal, IntArray(numDim) { marksPerDim })
        val marksImagNonUniform = MarksGenerator.getNonUniformMarks(dataImag, IntArray(numDim) { marksPerDim })
        val marksRealEquallyPopulated = MarksGenerator.getEquallyPopulatedMarks(dataReal, IntArray(numDim) { marksPerDim })
        val marksImagEquallyPopulated = MarksGenerator.getEquallyPopulatedMarks(dataImag, IntArray(numDim) { marksPerDim })

        val boundTightnessesUniform = data.indices.flatMap { i ->
            data.indices.map { j ->
                testComplexInnerProductBounds(dataReal[i], dataImag[i], dataReal[j], dataImag[j], marksReal, marksImag)
            }
        }
        val boundTightnessesNonUniform = data.indices.flatMap { i ->
            data.indices.map { j ->
                testComplexInnerProductBounds(dataReal[i], dataImag[i], dataReal[j], dataImag[j], marksRealNonUniform, marksImagNonUniform)
            }
        }
        // todo: investigate why this gives worse average results...
        val boundTightnessesEquallyPopulated = data.indices.flatMap { i ->
            data.indices.map { j ->
                testComplexInnerProductBounds(dataReal[i], dataImag[i], dataReal[j], dataImag[j], marksRealEquallyPopulated, marksImagEquallyPopulated)
            }
        }
        BoundTightness::class.declaredMemberProperties.filter { p -> p.returnType == Double::class.createType()}.forEach { p ->
            listOf(boundTightnessesUniform to "uniform",
                    boundTightnessesNonUniform to "nonUniform",
                    boundTightnessesEquallyPopulated to "equallyPopulated").forEach {
                println(it.second)
                val values = it.first.map(p).map { it as Double }
                val sum = values.sum()
                val avg = sum / values.size
                val max = values.max()!!
                val min = values.min()!!
                println(p.name)
                println("min: $min, avg: $avg, max: $max")
            }
        }
    }

    @ExperimentalTime
    @ParameterizedTest
    @ValueSource(ints = [5, 10, 20, 50])
    fun boundsSpeedFromFile(mpd: Int) {
        val data = getComplexVectorsFromFile("src/test/resources/sampledVectors.csv/", 1, numDim)
        val dataReal = data.map { vec -> vec.map { it.real.value }.toDoubleArray() }.toTypedArray()
        val dataImag = data.map { vec -> vec.map { it.imaginary.value }.toDoubleArray() }.toTypedArray()
        val marksReal = MarksGenerator.getEquidistantMarks(dataReal, IntArray(20) { mpd })
        val marksImag = MarksGenerator.getEquidistantMarks(dataImag, IntArray(20) { mpd })
        val cellsVecsReal = dataReal.map { marksReal.getCells(it) }.toTypedArray()
        val cellsVecsImag = dataImag.map { marksImag.getCells(it) }.toTypedArray()

        var timeExact = Duration.ZERO
        var timeUpper = Duration.ZERO
        var count = 0
        (data.indices).forEach { i ->
            (0  until data.size).forEach { j ->
                val query = data[j]
                val queryReal = dataReal[j]
                val queryImag = dataImag[j]
                timeExact += measureTime { AbsoluteInnerProductDistance(data[i], query) }
                val queryMarksProductRealReal = QueryMarkProducts(queryReal, marksReal)
                val queryMarksProductImagImag = QueryMarkProducts(queryImag, marksImag)
                val queryMarksProductRealImag = QueryMarkProducts(queryReal, marksImag)
                val queryMarksProductImagReal = QueryMarkProducts(queryImag, marksReal)
                timeUpper += measureTime {
                    absoluteComplexInnerProductSqUpperBoundCached2Public(cellsVecsReal[i],
                    cellsVecsImag[j],
                    queryMarksProductRealReal,
                    queryMarksProductImagImag,
                    queryMarksProductRealImag,
                    queryMarksProductImagReal)
                }
                count++
            }
        }
        println("marksPerDim $mpd: avg time exact ${timeExact.inMilliseconds / count} ms")
        println("marksPerDim $mpd: avg time upper ${timeUpper.inMilliseconds / count} ms")
    }

}