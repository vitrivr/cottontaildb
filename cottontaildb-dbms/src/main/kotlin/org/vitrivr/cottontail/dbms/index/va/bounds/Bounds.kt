package org.vitrivr.cottontail.dbms.index.va.bounds

import org.vitrivr.cottontail.core.values.types.RealVectorValue
import org.vitrivr.cottontail.dbms.index.va.signature.VAFSignature

/**
 * Interface for bounds calculation in a [org.vitrivr.cottontail.dbms.index.va.VAFIndex]
 *
 * References:
 * [1] Weber, R. and Blott, S., 1997. An approximation based data structure for similarity search (No. 9141, p. 416). Technical Report 24, ESPRIT Project HERMES.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed class Bounds {
    /** Reference to the query vector. */
    protected abstract val query: DoubleArray

    /** [VAFSignature] for the query [RealVectorValue]. */
    protected abstract val rq: VAFSignature

    /**
     * Checks if the given [VAFSignature] is a VA-SSA candidate according to [1] by comparing the
     * lower bounds estimation to the given threshold and returns true if so and false otherwise.
     *
     * @param signature The [VAFSignature] to check.
     * @return True if [VAFSignature] is a candidate, false otherwise.
     */
    abstract fun lb(signature: VAFSignature, threshold: Double = Double.MAX_VALUE): Double

    abstract fun ub(signature: VAFSignature): Double

    open fun bounds(signature: VAFSignature): Pair<Double,Double> = Pair(ub(signature),lb(signature))
}