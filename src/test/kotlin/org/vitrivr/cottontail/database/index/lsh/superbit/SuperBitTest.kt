package org.vitrivr.cottontail.database.index.lsh.superbit

import com.github.doyaaaaaken.kotlincsv.dsl.csvWriter
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.vitrivr.cottontail.math.basics.isApproximatelyTheSame
import org.vitrivr.cottontail.math.knn.metrics.AbsoluteInnerProductDistance
import org.vitrivr.cottontail.math.knn.metrics.RealInnerProductDistance
import org.vitrivr.cottontail.model.values.Complex64VectorValue
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.model.values.types.ComplexVectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.testutils.getComplexVectorsFromFile
import org.vitrivr.cottontail.testutils.getRandomComplexVectors
import org.vitrivr.cottontail.testutils.getRandomRealVectors
import java.io.File
import java.util.*

internal class SuperBitTest {

    companion object {
        val numDim = 20
        val numVecs = 100
        val Ns = 1 until 10
        val Ls = 1 until 10
        @JvmStatic
        fun provideConfigurationsForSB(): List<Arguments> {
            return Ns.flatMap { N ->
                Ls.map { L ->
                    Arguments.of(N, L)
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("provideConfigurationsForSB")
    fun signatureComplexUniform(N: Int, L: Int) {
        println("N: $N, L: $L")
        val outDir = File("testOut/complex64")
        val rng = Random(1234)
        val sb = SuperBit(N, L, 1234, SuperBit.SamplingMethod.UNIFORM, Complex64VectorValue.zero(20))
        val vectors = getRandomComplexVectors(rng, numVecs, numDim)
        compareNormalizedVectors(vectors as Array<VectorValue<*>>, sb, File(outDir, "complex64SBLSHSignaturesUniformN${N}L${L}.csv"), true)
    }

    @ParameterizedTest
    @MethodSource("provideConfigurationsForSB")
    fun signatureComplexGaussian(N: Int, L: Int) {
        println("N: $N, L: $L")
        val outDir = File("testOut/complex64")
        val rng = Random(1234)
        val sb = SuperBit(N, L, 1234, SuperBit.SamplingMethod.GAUSSIAN, Complex64VectorValue.zero(20))
        val vectors = getRandomComplexVectors(rng, numVecs, numDim)
        compareNormalizedVectors(vectors as Array<VectorValue<*>>, sb, File(outDir, "complex64SBLSHSignaturesGaussianN${N}L${L}.csv"), true)
    }


    @ParameterizedTest
    @MethodSource("provideConfigurationsForSB")
    fun signatureComplexGaussianFromFile(N: Int, L: Int) {
        println("N: $N, L: $L")
        val outDir = File("testOut/fromCsv")
        val sb = SuperBit(N, L, 1234, SuperBit.SamplingMethod.GAUSSIAN, Complex64VectorValue.zero(20))
        val vectors = getComplexVectorsFromFile("src/test/resources/sampledVectors.csv", 1, numDim)
        compareNormalizedVectors(vectors as Array<VectorValue<*>>, sb, File(outDir, "complex64SBLSHSignaturesGaussianN${N}L${L}_withoutImag.csv"), false)
    }

    @ParameterizedTest
    @MethodSource("provideConfigurationsForSB")
    fun signatureComplexWithImaginaryGaussianFromFile(N: Int, L: Int) {
        println("N: $N, L: $L")
        val outDir = File("testOut/fromCsv")
        val sb = SuperBit(N, L, 1234, SuperBit.SamplingMethod.GAUSSIAN, Complex64VectorValue.zero(20))
        val vectors = getComplexVectorsFromFile("src/test/resources/sampledVectors.csv", 1, numDim)
        compareNormalizedVectors(vectors as Array<VectorValue<*>>, sb, File(outDir, "complex64SBLSHSignaturesGaussianN${N}L${L}_withImag.csv"), true)
    }

    @ParameterizedTest
    @MethodSource("provideConfigurationsForSB")
    fun signatureComplexUniformFromFile(N: Int, L: Int) {
        println("N: $N, L: $L")
        val outDir = File("testOut/fromCsv")
        val sb = SuperBit(N, L, 1234, SuperBit.SamplingMethod.UNIFORM, Complex64VectorValue.zero(20))
        val vectors = getComplexVectorsFromFile("src/test/resources/sampledVectors.csv", 1, numDim)
        compareNormalizedVectors(vectors as Array<VectorValue<*>>, sb, File(outDir, "complex64SBLSHSignaturesUniformN${N}L${L}_withoutImag.csv"), false)
    }

    @ParameterizedTest
    @MethodSource("provideConfigurationsForSB")
    fun signatureComplexWithImaginaryUniformFromFile(N: Int, L: Int) {
        println("N: $N, L: $L")
        val outDir = File("testOut/fromCsv")
        val sb = SuperBit(N, L, 1234, SuperBit.SamplingMethod.UNIFORM, Complex64VectorValue.zero(20))
        val vectors = getComplexVectorsFromFile("src/test/resources/sampledVectors.csv", 1, numDim)
        compareNormalizedVectors(vectors as Array<VectorValue<*>>, sb, File(outDir, "complex64SBLSHSignaturesUniformN${N}L${L}_withImag.csv"), true)
    }

    @ParameterizedTest
    @MethodSource("provideConfigurationsForSB")
    fun signatureRealGaussian(N: Int, L: Int) {
        val outDir = File("testOut/real")
        println("N: $N, L: $L")
        val sb = SuperBit(N, L, 1234, SuperBit.SamplingMethod.GAUSSIAN, DoubleVectorValue.zero(20))
        val vectors = getRandomRealVectors(Random(1234), numVecs, numDim)
        compareNormalizedVectors(vectors as Array<VectorValue<*>>, sb, File(outDir, "doubleSBLSHSignaturesGaussianN${N}L${L}.csv"), false)
    }

    @ParameterizedTest
    @MethodSource("provideConfigurationsForSB")
    fun signatureRealUniform(N: Int, L: Int) {
        val outDir = File("testOut/real")
        println("N: $N, L: $L")
        val sb = SuperBit(N, L, 1234, SuperBit.SamplingMethod.UNIFORM, DoubleVectorValue.zero(20))
        val vectors = getRandomRealVectors(Random(1234), numVecs, numDim)
        compareNormalizedVectors(vectors as Array<VectorValue<*>>, sb, File(outDir, "doubleSBLSHSignaturesUniformN${N}L${L}.csv"), false)
    }


    private fun compareNormalizedVectors(vectors: Array<VectorValue<*>>, sb: SuperBit, outCsvFile: File, includeImaginary: Boolean) {
        val signatures = vectors.map {
            if (includeImaginary) {
                sb.signatureComplex(it as ComplexVectorValue<*>)
            }
            else {
                sb.signature(it)
            }
        }

        // perform chunked analysis representing stages (analyze signature in chunks of superbits
        // pendently. This can be thought of as banding described in the Mining Massive Datasets book.
        // return for all chunksizes if any of the chunks was identical
        val signatureChunksizes = (sb.N .. sb.L * sb.N step sb.N).toList().filter { (sb.L * sb.N) % it == 0 }

        outCsvFile.parentFile.mkdirs()
        csvWriter().open(outCsvFile) {
            val header = listOf("i", "j", "absoluteIPDist", "realIPDist", "hammingDist") + signatureChunksizes.map { "hasACommonSubSignatureAtChunksize$it" }
            writeRow(header)
            for (i in vectors.indices) {
                for (j in i until vectors.size) {
                    val a = vectors[i]
                    val b = vectors[j]
                    val d = AbsoluteInnerProductDistance(a, b)
                    val dreal = RealInnerProductDistance(a, b)
                    val hd = (signatures[i] zip signatures[j]).map { (a, b) ->
                        if (a == b) 0 else 1
                    }.sum()
                    val subSignatureCommonalities = signatureChunksizes.map { chunkSize ->
                        (signatures[i].toList().chunked(chunkSize) zip signatures[j].toList().chunked(chunkSize)).any {
                            (subSigi, subSigj) ->
                                subSigi == subSigj
                        }
                    }
                    val data = listOf(i, j, d.value, dreal.value, hd) + subSignatureCommonalities
                    writeRow(data)
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("provideConfigurationsForSB")
    fun testOrthogonalityRealGaussian(N: Int, L: Int) {
        testOrthogonality(N, L, DoubleVectorValue.zero(numDim), SuperBit.SamplingMethod.GAUSSIAN)
    }

    @ParameterizedTest
    @MethodSource("provideConfigurationsForSB")
    fun testOrthogonalityRealUniform(N: Int, L: Int) {
        testOrthogonality(N, L, DoubleVectorValue.zero(numDim), SuperBit.SamplingMethod.UNIFORM)
    }

    @ParameterizedTest
    @MethodSource("provideConfigurationsForSB")
    fun testOrthogonalityComplexGaussian(N: Int, L: Int) {
        testOrthogonality(N, L, Complex64VectorValue.zero(numDim), SuperBit.SamplingMethod.GAUSSIAN)
    }

    @ParameterizedTest
    @MethodSource("provideConfigurationsForSB")
    fun testOrthogonalityComplexUniform(N: Int, L: Int) {
        testOrthogonality(N, L, Complex64VectorValue.zero(numDim), SuperBit.SamplingMethod.UNIFORM)
    }

    private fun testOrthogonality(N: Int, L: Int, vec: Any, samplingMethod: SuperBit.SamplingMethod) {
        val sb = SuperBit(N, L, 1234, samplingMethod, vec as VectorValue<*>)
        for (l in 0 until L) {
            for (n in 1 until N) {
                isApproximatelyTheSame(0.0f,
                        (sb.hyperplanes[l * N] dot sb.hyperplanes[l * N + n]).abs().value)
            }
        }
    }
}