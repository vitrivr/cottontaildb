package org.vitrivr.cottontail.database.index.pq

import org.apache.commons.math3.ml.clustering.Clusterable
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.database.index.pq.clustering.KMeansClustererComplex

import org.vitrivr.cottontail.model.values.Complex64VectorValue
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import java.util.*

/**
 * @author: Gabriel Zihlmann, 5.10.2020
 */
internal class KMeansClustererComplexTest {

    /**
     * Compare our clusterer with the one of commons math
     */
    @Test
    fun cluster() {
        val rng = SplittableRandom(1324L)
        val numVecs = 100
        val dim = 2
        val k = 10
        val data = Array(numVecs) { DoubleVectorValue.random(dim, rng) }
        val dataC = data.map { Complex64VectorValue.zero(dim) + it }.toTypedArray()

        val maxIter = 1000
        val clusterer = KMeansPlusPlusClusterer<Vector>(k, maxIter)
        val clusters = clusterer.cluster(data.map { Vector(it.data) })
        clusters.forEachIndexed { i, it ->
            println("center$i")
            println(it.center.point.joinToString())
            println("points")
            it.points.forEach { p ->
                println(p.point.joinToString())
            }
        }
        val clusterer_ = KMeansClustererComplex<Complex64VectorValue>(k, rng) { a, b ->
            (a - b).norm2().value
        }
        val clusters_ = clusterer_.cluster(dataC, maxIter)
        clusters_.forEachIndexed { i, it ->
            println("center$i")
            println(it.center.map { centerComponent -> centerComponent.real.value }.joinToString())
            println("points")
            it.clusterPointIndices.forEach { j ->
                println(data[j].joinToString { it.value.toString() })
            }
        }
    }
    private class Vector(val data: DoubleArray): Clusterable {
        override fun getPoint() = data
    }
}