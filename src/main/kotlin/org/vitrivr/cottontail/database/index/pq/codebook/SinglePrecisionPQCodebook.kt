package org.vitrivr.cottontail.database.index.pq.codebook

import org.apache.commons.math3.stat.correlation.Covariance
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.database.column.ColumnType
import org.vitrivr.cottontail.database.column.FloatVectorColumnType
import org.vitrivr.cottontail.database.index.pq.PQSignature
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.model.values.FloatVectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * An [AbstractPQCodebook] implementation for [FloatVectorValue]s (single precision)
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.0.0
 */
class SinglePrecisionPQCodebook(centroids: Array<FloatVectorValue>, dataCovarianceMatrix: Array<FloatVectorValue>) : AbstractPQCodebook<FloatVectorValue>(centroids, dataCovarianceMatrix) {

    /** The [SinglePrecisionPQCodebook] handles [FloatVectorValue]s. */
    override val type: ColumnType<FloatVectorValue>
        get() = FloatVectorColumnType

    /**
     * Serializer object for [SinglePrecisionPQCodebook]
     */
    object Serializer : org.mapdb.Serializer<SinglePrecisionPQCodebook> {
        override fun serialize(out: DataOutput2, value: SinglePrecisionPQCodebook) {
            /* Serialize logical size of codebook entries. */
            out.packInt(value.logicalSize)
            val vectorSerializer = FloatVectorColumnType.serializer(value.logicalSize)

            /* Serialize centroids matrix. */
            out.packInt(value.centroids.size)
            for (v in value.centroids) {
                vectorSerializer.serialize(out, v)
            }

            /* Serialize covariance matrix. */
            out.packInt(value.dataCovarianceMatrix.size)
            for (v in value.dataCovarianceMatrix) {
                vectorSerializer.serialize(out, v)
            }
        }

        override fun deserialize(input: DataInput2, available: Int): SinglePrecisionPQCodebook {
            val logicalSize = input.unpackInt()
            val vectorSerializer = FloatVectorColumnType.serializer(logicalSize)
            val centroids = Array(input.unpackInt()) { vectorSerializer.deserialize(input, available) }
            val covMatrix = Array(input.unpackInt()) { vectorSerializer.deserialize(input, available) }
            return SinglePrecisionPQCodebook(centroids, covMatrix)
        }
    }

    companion object {
        /**
         * Learns the [PQCodebook] and the [PQSignature] and returns them. Internally, the clustering
         * is done with apache commons k-means++ in double precision but the returned codebook contains
         * centroids of the same datatype as was supplied.
         *
         * @param subspaceData The subspace, i.e., the vectors per subspace.
         * @param numCentroids The number of centroids to learn.
         * @param seed The random seed used for learning.
         * @param maxIterations The number of iterations to use.s
         */
        fun learnFromData(subspaceData: Array<FloatVectorValue>, numCentroids: Int, seed: Long, maxIterations: Int): SinglePrecisionPQCodebook {
            /* Prepare covariance matrix and centroid clusters. */
            val array = Array(subspaceData.size) { i ->
                DoubleArray(subspaceData[i].data.size) { j ->
                    subspaceData[i].data[j].toDouble()
                }
            }
            val covMatrix = Covariance(array, false).covarianceMatrix
            val centroidClusters = clusterRealData(array, covMatrix, numCentroids, seed, maxIterations)

            /* Convert to typed centroids and covariance matrix. */
            val centroids = Array(numCentroids) { FloatVectorValue(centroidClusters[it].center.point) }
            val dataCovMatrix = covMatrix.data.map { FloatVectorValue(it) }.toTypedArray()

            /* Return PQCodebook. */
            return SinglePrecisionPQCodebook(centroids, dataCovMatrix)
        }
    }

    /**
     * Calculates the squared mahalanobis distance between the given [DoubleVectorValue] and the
     * i-th centroid.
     *
     * Since usually, the centroids are smaller than the [DoubleVectorValue]s provided, only the part
     * of the vector that matches the given [index] is compared.
     *
     * @param v The [FloatVectorValue] to calculate the distance for.
     * @param ci The index of the centroid to compare to.
     *
     * @return Squared mahalanobis distance between the given [VectorValue] and the i-th centroid.
     */
    override fun squaredMahalanobis(v: FloatVectorValue, start: Int, ci: Int): Double {
        var dist = 0.0
        val diff = FloatVectorValue(FloatArray(this.logicalSize) {
            this.centroids[ci].data[it] - v.data[start + it]
        })
        for ((i, m) in this.dataCovarianceMatrix.withIndex()) {
            dist += diff.data[i] * (m.dot(diff).value)
        }
        return dist
    }
}