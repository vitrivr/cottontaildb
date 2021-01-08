package org.vitrivr.cottontail.database.index.va.bounds

import org.vitrivr.cottontail.database.index.va.signature.Marks
import org.vitrivr.cottontail.database.index.va.signature.Signature
import org.vitrivr.cottontail.model.values.types.RealVectorValue

/**
 * A [Bounds] implementation for inner product.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.0.1
 */
class InnerProductBounds(query: RealVectorValue<*>, marks: Marks) : Bounds {
    /** Lower bound of this [L2Bounds]. */
    override var lb = 0.0
        private set

    /** Upper bound of this [L2Bounds]. */
    override var ub = 0.0
        private set

    /** Cells for the query [RealVectorValue]. */
    private val rq = marks.getCells(query)

    /** Internal lookup table for pre-calculated values used in bounds calculation. */
    private val lat = Array(marks.marks.size) { dim ->
        DoubleArray(marks.marks[dim].size) { mark ->
            marks.marks[dim][mark] * query[dim].value.toDouble()
        }
    }

    /**
     * Updates the lower and upper bounds of this [L2Bounds] for the given [Signature].
     *
     * @param signature The [Signature] to calculate the bounds for.
     */
    override fun update(signature: Signature): LpBounds {
        TODO()
    }

    override fun isVASSACandidate(signature: Signature, threshold: Double): Boolean {
        TODO("Not yet implemented")
    }
}