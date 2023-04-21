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
     * Calculates and returns the lower bounds for this [Bounds].
     *
     * @param signature [VAFSignature] to calculate bounds for.
     * @param threshold Threshold for early abort.
     * @return Upper bound.
     */
    abstract fun lb(signature: VAFSignature, threshold: Double = Double.MAX_VALUE): Double

    /**
     * Calculates and returns the upper bounds for this [Bounds].
     *
     * @param signature [VAFSignature] to calculate bounds for.
     * @param threshold Threshold for early abort.
     * @return Upper bound.
     */
    abstract fun ub(signature: VAFSignature, threshold: Double = Double.MAX_VALUE): Double

    /**
     * Calculates and returns the bounds for this [Bounds].
     *
     * @param signature [VAFSignature] to calculate bounds for.
     * @return Bounds
     */
    abstract fun bounds(signature: VAFSignature): Pair<Double,Double>
}