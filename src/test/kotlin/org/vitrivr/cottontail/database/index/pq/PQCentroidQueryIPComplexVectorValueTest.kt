package org.vitrivr.cottontail.database.index.pq

import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.model.values.Complex32Value
import org.vitrivr.cottontail.model.values.Complex32VectorValue
import java.util.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

internal class PQCentroidQueryIPComplexVectorValueTest {
    @Test
    @ExperimentalTime
    @ExperimentalUnsignedTypes
    fun testApproximateIPs() {
        val numCentroids = 2048
        val numSubspaces = 10
        val rng = Random(1234L)
        val testObjOld = PQCentroidQueryIPComplexVectorValue(Array(numSubspaces) { i ->
            Complex32VectorValue(Array(numCentroids) {
                Complex32Value(rng.nextGaussian(), rng.nextGaussian())
            })
        })
        val testObjNew = PQCentroidQueryIPComplexVectorValue(Array(numSubspaces) { i ->
            Complex32VectorValue(Array(numCentroids) {
                Complex32Value(rng.nextGaussian(), rng.nextGaussian())
            })
        })

        val numSigs = 9e6.toInt()
        val signaturesOld = UShortArray(numSubspaces * numSigs) {
            rng.nextInt(numCentroids).toUShort()
        }
        val signaturesNew = UShortArray(numSubspaces * numSigs) {
            rng.nextInt(numCentroids).toUShort()
        }
        val signatureOffsetsOld = IntArray(numSigs) {
            rng.nextInt(numSigs * numSubspaces - numSubspaces)
        }
        val signatureOffsetsNew = IntArray(numSigs) {
            rng.nextInt(numSigs * numSubspaces - numSubspaces)
        }

        var timeOld = Duration.ZERO
        var timeNew = Duration.ZERO

        // also test in reverse order! Caching and JIT, etc...
        signatureOffsetsNew.forEach {
            timeNew += measureTime { testObjNew.approximateIP(signaturesNew, it, numSubspaces) }
        }
        signatureOffsetsOld.forEach {
            timeOld += measureTime { testObjOld.approximateIPOld(signaturesOld, it, numSubspaces) }
        }
        println("TimeOld: $timeOld")
        println("TimeNew: $timeNew")
        println("Speedup: ${timeNew / timeOld}")
    }
}