package org.vitrivr.cottontail.database.index.va.bounds

import org.vitrivr.cottontail.database.index.va.signature.Marks
import org.vitrivr.cottontail.database.index.va.signature.Signature
import org.vitrivr.cottontail.model.values.types.RealVectorValue
import kotlin.math.max

/**
 * A [Bounds] implementation for L1 (Manhattan) distance.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class L1Bounds(query: RealVectorValue<*>, marks: Marks) : Bounds {

    /** Lower bound of this [L1Bounds]. */
    override var lb = 0.0
        private set

    /** Upper bound of this [L1Bounds]. */
    override var ub = 0.0
        private set

    /** Cells for the query [RealVectorValue]. */
    private val rq = marks.getCells(query)

    /** Internal lookup table for pre-calculated values used in bounds calculation. */
    private val lat = Array(marks.marks.size) { j ->
        val qj = query[j].value.toDouble()
        Array(marks.marks[j].size) { m ->
            doubleArrayOf((qj - marks.marks[j][m]), (marks.marks[j][m] - qj))
        }
    }

    /**
     * Updates the lower and upper bounds of this [L1Bounds] for the given [Signature].
     *
     * @param signature The [Signature] to calculate the bounds for.
     */
    override fun update(signature: Signature) {
        this.lb = 0.0
        this.ub = 0.0
        val ri = signature.cells
        for (j in signature.cells.indices) {
            when {
                ri[j] < this.rq[j] -> {
                    this.lb += this.lat[j][ri[j] + 1][0]
                    this.ub += this.lat[j][ri[j]][0]
                }
                ri[j] == this.rq[j] -> {
                    this.ub += max(this.lat[j][ri[j]][0], this.lat[j][ri[j] + 1][1])
                }
                ri[j] > this.rq[j] -> {
                    this.lb += this.lat[j][ri[j]][1]
                    this.ub += this.lat[j][ri[j] + 1][1]
                }
            }
        }
    }
}