package org.vitrivr.cottontail.database.index.pq.codebook

import org.apache.commons.math3.linear.RealMatrix
import org.apache.commons.math3.ml.clustering.CentroidCluster
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer
import org.apache.commons.math3.random.JDKRandomGenerator
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * An abstract [PQCodebook] implementation that can be used to quantize a [VectorValue]. Centroids
 * are learned by k-means clustering using the mahalanobis distance between sample points.
 *
 * Used for product quantization based index structures.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.0.0
 */
abstract class AbstractPQCodebook<T : VectorValue<*>>(protected val centroids: Array<T>, protected val dataCovarianceMatrix: Array<T>) : PQCodebook<T> {

    companion object {
        /**
         * Calculates and returns the squared mahalanobis distance between the points represented by
         * [DoubleArray] a and b using the given covariance matrix.
         *
         * @param a The first [DoubleArray].
         * @param b The second [DoubleArray].
         * @param covMatrix The covariance [RealMatrix].
         */
        fun squaredMahalanobis(a: DoubleArray, b: DoubleArray, covMatrix: RealMatrix): Double {
            require(a.size == b.size) { }
            var dist = 0.0
            val diff = DoubleArray(a.lastIndex) { a[it] - b[it] }
            for (i in 0 until covMatrix.columnDimension) {
                var h = 0.0
                for (j in diff.indices) {
                    h += diff[j] * covMatrix.getEntry(i, j)
                }
                dist += h * diff[i]
            }
            return dist
        }

        /**
         * Clusters a series data points partitioned into subspaces to a pre-defined number of centroids
         * using k-means clustering.
         *
         * @param data An array of [DoubleArray]s containing the subspace data.
         * @param covMatrix The covariance [RealMatrix].
         * @param numCentroids The desired number of centroids.
         * @param seed The random number seed.
         * @param maxIterations The maximum number of iterations to use for the clustering.
         */
        fun clusterRealData(data: Array<DoubleArray>, covMatrix: RealMatrix, numCentroids: Int, seed: Long, maxIterations: Int): MutableList<CentroidCluster<ClusterableWithIndex>> {
            /* Learn clusters using KMeans clustering. */
            val measure: (a: DoubleArray, b: DoubleArray) -> Double = { a, b -> squaredMahalanobis(a, b, covMatrix) }
            val clusterer = KMeansPlusPlusClusterer<ClusterableWithIndex>(numCentroids, maxIterations, measure, JDKRandomGenerator(seed.toInt()))
            val centroidClusters = clusterer.cluster(data.mapIndexed { i, value ->
                object : ClusterableWithIndex {
                    override fun getIndex() = i
                    override fun getPoint(): DoubleArray = value
                }
            })
            return centroidClusters
        }
    }

    /** The number of centroids contained in this [AbstractPQCodebook]. */
    override val numberOfCentroids: Int
        get() = this.centroids.size

    /** The logical size of the centroids held by this [AbstractPQCodebook]. */
    override val logicalSize: Int
        get() = this.centroids[0].logicalSize

    /**
     * Returns the centroid [VectorValue] for the given index.
     *
     * @param ci The index of the centroid to return.
     * @return The [DoubleVectorValue] representing the centroid for the given index.
     */
    override fun get(ci: Int): T = this.centroids[ci]

    /**
     * Quantizes the given [VectorValue] and returns the index of the centroid it belongs to. Distance
     * calculation starts from the given [start] vector component and considers [logicalSize] components.
     *
     * @param v The [VectorValue] to quantize.
     * @param start The index of the first [VectorValue] component to consider for distance calculation.
     * @return The index of the centroid the given [VectorValue] belongs to.
     */
    override fun quantizeSubspaceForVector(v: T, start: Int): Int {
        var mahIndex = 0
        var mah = Double.POSITIVE_INFINITY
        for (c in this.centroids.indices) {
            val m = squaredMahalanobis(v, start, c)
            if (m < mah) {
                mah = m
                mahIndex = c
            }
        }
        return mahIndex
    }
}