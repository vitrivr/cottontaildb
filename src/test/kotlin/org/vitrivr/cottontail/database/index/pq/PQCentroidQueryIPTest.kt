package org.vitrivr.cottontail.database.index.pq

import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.math.basics.isApproximatelyTheSame
import org.vitrivr.cottontail.testutils.getComplexVectorsFromFile
import org.vitrivr.cottontail.testutils.sampleVectorsFromCsv
import java.io.File
import java.util.*
import kotlin.time.ExperimentalTime

internal class PQCentroidQueryIPTest {

    @ExperimentalUnsignedTypes
    @ExperimentalTime
    @Test
    fun testPQCentroidQuery() {
        val vectorsFile = File("src/test/resources/sampledVectors90000.csv")
        if (!vectorsFile.exists()) {
            sampleVectorsFromCsv("src/test/resources/complexVectors.csv", false, vectorsFile.toString(), Random(1234L), 1e-2)
        }
        val data = getComplexVectorsFromFile(vectorsFile.toString(), 1, 20)
        val realData = Array(data.size) { i ->
            DoubleArray(data[i].logicalSize) { j ->
                data[i][j].real.value
            }
        }
        val imagData = Array(data.size) { i ->
            DoubleArray(data[i].logicalSize) { j ->
                data[i][j].imaginary.value
            }
        }
        val numSubspaces = 4
        val numCentroids = 128
        val seed = 1234L
        val rng = SplittableRandom(seed)
        val (permutation, reversePermutation) = PQIndex.generateRandomPermutation(realData[0].size, rng)
        val permutedRealData = Array(realData.size) { n ->
            DoubleArray(realData[n].size) { i ->
                realData[n][permutation[i]]
            }
        }
        val pqReal = PQ.fromPermutedData(numSubspaces, numCentroids, permutedRealData, null, seed)

        println("Comparing precomputed and direct IP approximations")
        for (i in data.indices) {
            val centroidQueryIP = pqReal.first.precomputeCentroidQueryIP(permutedRealData[i])
            if (i % 1000 == 0) {
                if (i % 10000 == 0) {
                    println("$i")
                } else {
                    print(".")
                }
            }
            for (j in i until data.size) {
                if (j % 10 == 0) {
                    val approxRealReal = pqReal.first.approximateAsymmetricIP(pqReal.second[j], permutedRealData[i])
                    val approxRealRealPrecomp = centroidQueryIP.approximateIP(pqReal.second[j])
                    isApproximatelyTheSame(approxRealReal.toFloat(), approxRealRealPrecomp)
                }
            }
        }
    }
}