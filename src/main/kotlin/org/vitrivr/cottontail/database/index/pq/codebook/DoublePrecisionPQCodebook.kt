package org.vitrivr.cottontail.database.index.pq.codebook

import org.apache.commons.math3.stat.correlation.Covariance
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.database.column.*
import org.vitrivr.cottontail.database.index.pq.PQSignature
import org.vitrivr.cottontail.database.index.pq.codebook.PQCodebook.Companion.clusterRealData
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * An [AbstractPQCodebook] implementation for [DoubleVectorValue]s
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.0.0
 */
class DoublePrecisionPQCodebook(
    protected val centroids: List<DoubleVectorValue>,
    protected val covMatrix: List<DoubleVectorValue>
) : PQCodebook<DoubleVectorValue> {

    /**
     * Serializer object for [DoublePrecisionPQCodebook]
     */
    object Serializer : org.mapdb.Serializer<DoublePrecisionPQCodebook> {
        override fun serialize(out: DataOutput2, value: DoublePrecisionPQCodebook) {
            /* Serialize logical size of codebook entries. */
            out.packInt(value.logicalSize)
            val vectorSerializer = DoubleVectorColumnType.serializer(value.logicalSize)

            /* Serialize centroids matrix. */
            out.packInt(value.centroids.size)
            for (v in value.centroids) {
                vectorSerializer.serialize(out, v)
            }

            /* Serialize covariance matrix. */
            out.packInt(value.covMatrix.size)
            for (v in value.covMatrix) {
                vectorSerializer.serialize(out, v)
            }
        }

        override fun deserialize(input: DataInput2, available: Int): DoublePrecisionPQCodebook {
            val logicalSize = input.unpackInt()
            val vectorSerializer = DoubleVectorColumnType.serializer(logicalSize)
            val centroids = ArrayList<DoubleVectorValue>(input.unpackInt())
            for (i in 0 until centroids.size) {
                centroids.add(vectorSerializer.deserialize(input, available))
            }
            val covMatrix = ArrayList<DoubleVectorValue>(input.unpackInt())
            for (i in 0 until covMatrix.size) {
                covMatrix.add(vectorSerializer.deserialize(input, available))
            }
            return DoublePrecisionPQCodebook(centroids, covMatrix)
        }
    }

    companion object {

        /**
         * Learns the [DoublePrecisionPQCodebook] and the [PQSignature] and returns them. Internally,
         * the clustering is done with apache commons k-means++ in double precision but the returned
         * codebook contains centroids of the same datatype as was supplied.
         *
         * @param subspaceData The subspace, i.e., the vectors per subspace.
         * @param numCentroids The number of centroids to learn.
         * @param seed The random seed used for learning.
         * @param maxIterations The number of iterations to use.s
         */
        fun learnFromData(
            subspaceData: List<DoubleVectorValue>,
            numCentroids: Int,
            seed: Long,
            maxIterations: Int
        ): DoublePrecisionPQCodebook {
            /* Prepare covariance matrix and centroid clusters. */
            val array = Array(subspaceData.size) { i -> subspaceData[i].data }
            val covMatrix = Covariance(array, false).covarianceMatrix
            val centroidClusters =
                clusterRealData(array, covMatrix, numCentroids, seed, maxIterations)

            /* Convert to typed centroids and covariance matrix. */
            val centroids = ArrayList<DoubleVectorValue>(numCentroids)
            for (i in 0 until numCentroids) {
                centroids.add(DoubleVectorValue(centroidClusters[i].center.point))
            }
            val dataCovMatrix = covMatrix.data.map { DoubleVectorValue(it) }

            /* Return PQCodebook. */
            return DoublePrecisionPQCodebook(centroids, dataCovMatrix)
        }
    }

    /** The [DoublePrecisionPQCodebook] handles [DoubleVectorColumnType]s. */
    override val type: ColumnType<DoubleVectorValue>
        get() = DoubleVectorColumnType

    /** The number of centroids contained in this [SinglePrecisionPQCodebook]. */
    override val numberOfCentroids: Int
        get() = this.centroids.size

    /** The logical size of the centroids held by this [SinglePrecisionPQCodebook]. */
    override val logicalSize: Int
        get() = this.centroids[0].logicalSize

    /**
     * Returns the centroid [VectorValue] for the given index.
     *
     * @param ci The index of the centroid to return.
     * @return The [DoubleVectorValue] representing the centroid for the given index.
     */
    override fun get(ci: Int): DoubleVectorValue = this.centroids[ci]

    /**
     * Quantizes the given [DoubleVectorValue] and returns the index of the centroid it belongs to.
     * Distance calculation starts from the given [start] vector component and considers [logicalSize]
     * components.
     *
     * @param v The [DoubleVectorValue] to quantize.
     * @param start The index of the first component to consider for distance calculation.
     * @return The index of the centroid the given [VectorValue] belongs to.
     */
    override fun quantizeSubspaceForVector(v: DoubleVectorValue, start: Int): Byte {
        var mahIndex = 0
        var mah = Double.POSITIVE_INFINITY
        var i = 0
        val diff = DoubleVectorValue(DoubleArray(this.logicalSize))
        outer@ for (c in this.centroids) {
            var dist = 0.0
            for (it in diff.data.indices) {
                diff.data[it] = c.data[it] - v.data[start + it]
            }
            var j = 0
            for (m in this.covMatrix) {
                dist = Math.fma(diff.data[j++], (m.dot(diff).value), dist)
                if (dist >= mah) {
                    i++
                    continue@outer
                }
            }
            if (dist < mah) {
                mah = dist
                mahIndex = i
            }
            i++
        }
        return mahIndex.toByte()
    }
}