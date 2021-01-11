package org.vitrivr.cottontail.database.index.pq.codebook

import org.vitrivr.cottontail.database.column.ColumnType
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * A codebook that can be used to quantize a [VectorValue] (or more precisely, a subspace thereof)
 * to a learned centroid. Used for product quantization based index structures.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface PQCodebook<T : VectorValue<*>> {

    /** The [ColumnType] of the vectors contained in this [PQCodebook]. */
    val type: ColumnType<T>


    /** The number of centroids contained in this [PQCodebook]. */
    val numberOfCentroids: Int

    /** The logical size the [VectorValue]s contained in this [PQCodebook]. */
    val logicalSize: Int

    /**
     * Returns the centroid [VectorValue] for [ci] (the ci-th centroid).
     *
     * @param ci The index to return the [VectorValue] for.
     * @return The [VectorValue] representing the centroid for the given index.
     */
    operator fun get(ci: Int): T

    /**
     * Quantizes the given [VectorValue] and returns the index of the centroid it belongs to. Distance
     * calculation starts from the given [start] vector component and considers [logicalSize] components.
     *
     * @param v The [VectorValue] to quantize.
     * @param start The index of the first [VectorValue] component to consider for distance calculation.
     * @return The index of the centroid the given [VectorValue] belongs to.
     */
    fun quantizeSubspaceForVector(v: T, start: Int): Int

    /**
     * Calculates the squared mahalanobis distance between the given [VectorValue] and the ci-th centroid.
     *
     * Since usually, the centroids are smaller than the [VectorValue]s provided, only the part of
     * the vector that matches the given [ci] is compared.
     *
     * @param v The [VectorValue] to calculate the distance for.
     * @param ci The index of the centroid to compare to.
     *
     * @return Squared mahalanobis distance between the given [VectorValue] and the i-th centroid.
     */
    fun squaredMahalanobis(v: T, start: Int, ci: Int): Double
}