package org.vitrivr.cottontail.dbms.index.pq.quantizer

import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.types.VectorValue

/**
 * A codebook that can be used to quantize a [VectorValue] (or more precisely, a subspace thereof)
 * to a learned centroid. Used for product quantization based index structures.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
data class PQCodebook(val distance: VectorDistance<*>, val centroids: Array<VectorValue<*>>) {

    init {
        require(this.centroids.all { it.logicalSize == this.distance.type.logicalSize }) { "Dimensionality of centroids and distance function do not match for PQ codebook." }
    }

    /** The size of the subspace encoded by this [PQCodebook]. */
    val subspaceSize: Int
        get() = this.centroids[0].logicalSize

    /** The number of centroids contained in this [PQCodebook]. */
    val numberOfCentroids: Int
        get() = this.centroids.size

    /**
     * Quantizes the given vector [VectorValue] and returns the index of the centroid it belongs to.
     *
     * @param vector The vector [VectorValue] to quantize.
     * @return The index of the centroid the given [VectorValue] belongs to.
     */
    fun quantize(vector: VectorValue<*>): Int {
        var smallestIndexSeen = 0
        var smallestDistSeen = this.distance(vector, this.centroids[0])!!.value
        for (i in 1 until this.centroids.size) {
            val newDist = this.distance(vector, this.centroids[i])!!.value
            if (newDist < smallestDistSeen) {
                smallestDistSeen = newDist
                smallestIndexSeen = i
            }
        }
        return smallestIndexSeen
    }

    /**
     * Calculates the distance of the given [VectorValue] from the centroid with the given index.
     *
     * @param vector The [VectorValue] to evalute.
     * @param centroidIndex The centroid index.
     */
    fun distanceFrom(vector: VectorValue<*>, centroidIndex: Int): Double {
        require(vector.logicalSize == this.subspaceSize) { "Dimension mismatch between sub-vector and codebook." }
        return this.distance(vector, this.centroids[centroidIndex])!!.value
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is PQCodebook) return false
        if (this.distance.signature != other.distance.signature) return false
        if (this.centroids.size != other.centroids.size) return false
        for (i in 0 until this.centroids.size) {
            for (j in 0 until this.centroids[i].logicalSize) {
                if (this.centroids[i][j] != other.centroids[i][j]) return false
            }
        }
        return true
    }

    override fun hashCode(): Int {
        var result = this.distance.signature.hashCode()
        result = 31 * result + this.centroids.contentHashCode()
        return result
    }
}