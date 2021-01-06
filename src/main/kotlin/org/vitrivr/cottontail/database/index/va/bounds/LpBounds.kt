package org.vitrivr.cottontail.database.index.va.bounds

import org.vitrivr.cottontail.database.index.va.signature.Marks
import org.vitrivr.cottontail.database.index.va.signature.Signature
import org.vitrivr.cottontail.model.values.types.RealVectorValue
import kotlin.math.max
import kotlin.math.pow

/**
 * A [Bounds] implementation for Lp distance.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LpBounds(query: RealVectorValue<*>, marks: Marks, p: Int) : Bounds {

    /** Lower bound of this [LpBounds]. */
    override var lb = 0.0
        private set

    /** Upper bound of this [LpBounds]. */
    override var ub = 0.0
        private set

    /** Exponent used for p-th root calculation. */
    private val exp = 1.0 / p

    /** Cells for the query [RealVectorValue]. */
    private val rq = marks.getCells(query)

    /** Internal lookup table for pre-calculated values used in bounds calculation. */
    private val lat = Array(marks.marks.size) { j ->
        val qj = query[j].value.toDouble()
        Array(marks.marks[j].size) { m ->
            doubleArrayOf((qj - marks.marks[j][m]).pow(p), (marks.marks[j][m] - qj).pow(p))
        }
    }

    /**
     * Updates the lower and upper bounds of this [L2Bounds] for the given [Signature].
     *
     * @param signature The [Signature] to calculate the bounds for.
     */
    override fun update(signature: Signature) {
        var lb = 0.0
        var ub = 0.0
        val ri = signature.cells
        for (j in signature.cells.indices) {
            when {
                ri[j] < this.rq[j] -> {
                    lb += this.lat[j][ri[j] + 1][0]
                    ub += this.lat[j][ri[j]][0]
                }
                ri[j] == this.rq[j] -> {
                    ub += max(this.lat[j][ri[j]][0], this.lat[j][ri[j] + 1][1])
                }
                ri[j] > this.rq[j] -> {
                    lb += this.lat[j][ri[j]][1]
                    ub += this.lat[j][ri[j] + 1][1]
                }
            }
        }
        this.lb = lb.pow(this.exp)
        this.ub = ub.pow(this.exp)
    }
}