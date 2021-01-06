package org.vitrivr.cottontail.database.index.lsh.superbit

import com.github.doyaaaaaken.kotlincsv.dsl.csvWriter
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.vitrivr.cottontail.math.knn.metrics.AbsoluteInnerProductDistance
import org.vitrivr.cottontail.math.knn.metrics.RealInnerProductDistance
import org.vitrivr.cottontail.model.values.Complex64VectorValue
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.testutils.getComplexVectorsFromFile
import org.vitrivr.cottontail.testutils.getRandomComplexVectors
import org.vitrivr.cottontail.testutils.getRandomRealVectors
import org.vitrivr.cottontail.testutils.sampleVectorsFromCsv
import java.io.File
import java.util.*
import kotlin.time.ExperimentalTime

internal class SuperBitLSHTest {
    companion object {
        private val stages = arrayOf(1, 2)
        private val buckets = arrayOf(4, 16, 32, 64, 128, 256, 512)
        private val seeds = arrayOf(1234L, 4321L, 82134L, 1337L, 42L)
        private val numVectors = 100

        @JvmStatic
        fun provideConfigurationsForSBLSH(): List<Arguments> {
            return stages.flatMap { s ->
                buckets.flatMap { b ->
                    seeds.slice(0 until 1).map { seed ->
                        Arguments.of(s, b, seed)
                    }
                }
            }
        }
    }

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("provideConfigurationsForSBLSH")
    fun testRandomComplex64VectorsWithImaginaryUniform(stages: Int, buckets: Int, seed: Long) {
        println("stages: $stages buckets: $buckets seed: $seed")
        val rng = Random(seed)
        val numDim = 20
        val lsh = SuperBitLSH(stages, buckets, numDim, seed, Complex64VectorValue.zero(numDim), true, SuperBit.SamplingMethod.UNIFORM)
        println("L${lsh.superBit.L}N${lsh.superBit.N}sampling${lsh.superBit.samplingMethod}")
        val vectors = getRandomComplexVectors(rng, numVectors, numDim)
        compareNormalizedVectors(vectors as Array<VectorValue<*>>, lsh,
                File("testOut", "complex64/complexVectorsBucketDistancesUniform_stages${stages}buckets${buckets}seed${seed}_withImag.csv"))
    }

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("provideConfigurationsForSBLSH")
    fun testRandomComplex64VectorsUniform(stages: Int, buckets: Int, seed: Long) {
        println("stages: $stages buckets: $buckets seed: $seed")
        val rng = Random(seed)
        val numDim = 20
        val lsh = SuperBitLSH(stages, buckets, numDim, seed, Complex64VectorValue.zero(numDim), false, SuperBit.SamplingMethod.UNIFORM)
        println("L${lsh.superBit.L}N${lsh.superBit.N}sampling${lsh.superBit.samplingMethod}")
        val vectors = getRandomComplexVectors(rng, numVectors, numDim)
        compareNormalizedVectors(vectors as Array<VectorValue<*>>, lsh,
                File("testOut", "complex64/complexVectorsBucketDistancesUniform_stages${stages}buckets${buckets}seed${seed}_withoutImag.csv"))
    }

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("provideConfigurationsForSBLSH")
    fun testRandomComplex64VectorsGaussian(stages: Int, buckets: Int, seed: Long) {
        println("stages: $stages buckets: $buckets seed: $seed")
        val rng = Random(seed)
        val numDim = 20
        val lsh = SuperBitLSH(stages, buckets, numDim, seed, Complex64VectorValue.zero(numDim), false, SuperBit.SamplingMethod.GAUSSIAN)
        println("L${lsh.superBit.L}N${lsh.superBit.N}sampling${lsh.superBit.samplingMethod}")
        val vectors = getRandomComplexVectors(rng, numVectors, numDim)
        compareNormalizedVectors(vectors as Array<VectorValue<*>>, lsh,
            File("testOut", "complex64/complexVectorsBucketDistancesGaussian_stages${stages}buckets${buckets}seed${seed}_withoutImag.csv"))
    }

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("provideConfigurationsForSBLSH")
    fun testRandomComplex64VectorsWithImaginaryGaussian(stages: Int, buckets: Int, seed: Long) {
        println("stages: $stages buckets: $buckets seed: $seed")
        val rng = Random(seed)
        val numDim = 20
        val lsh = SuperBitLSH(stages, buckets, numDim, seed, Complex64VectorValue.zero(numDim), true, SuperBit.SamplingMethod.GAUSSIAN)
        println("L${lsh.superBit.L}N${lsh.superBit.N}sampling${lsh.superBit.samplingMethod}")
        val vectors = getRandomComplexVectors(rng, numVectors, numDim)
        compareNormalizedVectors(vectors as Array<VectorValue<*>>, lsh,
                File("testOut", "complex64/complexVectorsBucketDistancesGaussian_stages${stages}buckets${buckets}seed${seed}_withImag.csv"))
    }

    private fun compareNormalizedVectors(vectors: Array<VectorValue<*>>, lsh: SuperBitLSH, outCSVFile: File) {
        outCSVFile.parentFile.mkdirs()
        val bucketSignatures = vectors.map {
            lsh.hash(it)
        }

        // compare pair-wise bucket signature and IP distances
        csvWriter().open(outCSVFile) {
        writeRow(listOf("i", "j", "absoluteIPDist", "realIPDist", "numDiffBuckets", "superbitN", "superbitL"))
            for (i in vectors.indices) {
                for (j in i until vectors.size) {
                    val a = vectors[i]
                    val b = vectors[j]
                    val d = AbsoluteInnerProductDistance(a, b)
                    val r = RealInnerProductDistance(a, b)
                    val d2 = (bucketSignatures[i] zip bucketSignatures[j]).map { (a, b) ->
                        if (a == b) 0 else 1
                    }.sum()
                    writeRow(listOf(i, j, d.value, r.value, d2, lsh.superBit.N, lsh.superBit.L))
                }
            }
        }
    }

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("provideConfigurationsForSBLSH")
    fun testRandomDoubleVectors(stages: Int, buckets: Int, seed: Long) {
        println("stages: $stages buckets: $buckets seed: $seed")
        val rng = Random(seed)
        val numDim = 20
        val lsh = SuperBitLSH(stages, buckets, numDim, seed, DoubleVectorValue.zero(numDim), false, SuperBit.SamplingMethod.UNIFORM)
        val vectors = getRandomRealVectors(rng, numVectors, numDim)
        compareNormalizedVectors(vectors as Array<VectorValue<*>>, lsh, File("testOut",
                "real/realVectorsBucketDistances_stages${stages}buckets${buckets}seed${seed}.csv"))
    }

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("provideConfigurationsForSBLSH")
    fun testComplexVectorsFromFileUniform(stages: Int, buckets: Int, seed: Long) {
        println("stages: $stages buckets: $buckets seed: $seed")
        val numDim = 20
        val lsh = SuperBitLSH(stages, buckets, numDim, seed, DoubleVectorValue.zero(numDim), false, SuperBit.SamplingMethod.UNIFORM)
        val file = File("src/test/resources/sampledVectors.csv")
        if (!file.exists()) {
            sampleVectorsFromCsv("src/test/resources/complexVectors.csv", false, "src/test/resources/sampledVectors.csv", Random())
        }
        val vectors = getComplexVectorsFromFile(file.toString(), 1, numDim)
        compareNormalizedVectors(vectors as Array<VectorValue<*>>, lsh,
            File("testOut", "fromCsv/CsvVectorsBucketDistancesUniform_stages${stages}buckets${buckets}seed${seed}_withoutImag.csv"))
    }

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("provideConfigurationsForSBLSH")
    fun testComplexVectorsFromFileWithImaginaryUniform(stages: Int, buckets: Int, seed: Long) {
        println("stages: $stages buckets: $buckets seed: $seed")
        val numDim = 20
        val lsh = SuperBitLSH(stages, buckets, numDim, seed, DoubleVectorValue.zero(numDim), true, SuperBit.SamplingMethod.UNIFORM)
        val file = File("src/test/resources/sampledVectors.csv")
        if (!file.exists()) {
            sampleVectorsFromCsv("src/test/resources/complexVectors.csv", false, "src/test/resources/sampledVectors.csv", Random())
        }
        val vectors = getComplexVectorsFromFile(file.toString(), 1, numDim)
        compareNormalizedVectors(vectors as Array<VectorValue<*>>, lsh,
                File("testOut", "fromCsv/CsvVectorsBucketDistancesUniform_stages${stages}buckets${buckets}seed${seed}_withImag.csv"))
    }

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("provideConfigurationsForSBLSH")
    fun testComplexVectorsFromFileGaussian(stages: Int, buckets: Int, seed: Long) {
        println("stages: $stages buckets: $buckets seed: $seed")
        val numDim = 20
        val lsh = SuperBitLSH(stages, buckets, numDim, seed, DoubleVectorValue.zero(numDim), false, SuperBit.SamplingMethod.GAUSSIAN)
        val file = File("src/test/resources/sampledVectors.csv")
        if (!file.exists()) {
            sampleVectorsFromCsv("src/test/resources/complexVectors.csv", false, "src/test/resources/sampledVectors.csv", Random())
        }
        val vectors = getComplexVectorsFromFile(file.toString(), 1, numDim)
        compareNormalizedVectors(vectors as Array<VectorValue<*>>, lsh,
                File("testOut", "fromCsv/CsvVectorsBucketDistancesGaussian_stages${stages}buckets${buckets}seed${seed}_withoutImag.csv"))
    }

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("provideConfigurationsForSBLSH")
    fun testComplexVectorsFromFileWithImaginaryGaussian(stages: Int, buckets: Int, seed: Long) {
        println("stages: $stages buckets: $buckets seed: $seed")
        val numDim = 20
        val lsh = SuperBitLSH(stages, buckets, numDim, seed, DoubleVectorValue.zero(numDim), true, SuperBit.SamplingMethod.GAUSSIAN)
        val file = File("src/test/resources/sampledVectors.csv")
        if (!file.exists()) {
            sampleVectorsFromCsv("src/test/resources/complexVectors.csv", false, "src/test/resources/sampledVectors.csv", Random())
        }
        val vectors = getComplexVectorsFromFile(file.toString(), 1, numDim)
        compareNormalizedVectors(vectors as Array<VectorValue<*>>, lsh,
                File("testOut", "fromCsv/CsvVectorsBucketDistancesGaussian_stages${stages}buckets${buckets}seed${seed}_withImag.csv"))
    }

}