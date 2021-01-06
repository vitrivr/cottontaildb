package org.vitrivr.cottontail.database.index.va.bounds

import org.vitrivr.cottontail.database.index.va.signature.Signature

/**
 * Interface for bounds calculation in a [org.vitrivr.cottontail.database.index.va.VAFIndex]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface Bounds {
    /** Lower bound of this [Bounds]. */
    val lb: Double

    /** Upper bound of this [Bounds]. */
    val ub: Double

    /**
     * Updates the lower and upper bounds of this [Bounds] for the given [Signature].
     *
     * @param signature The [Signature] to calculate the bounds for.
     */
    fun update(signature: Signature)
}