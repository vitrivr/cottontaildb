package org.vitrivr.cottontail.dbms.index.va.bounds

import org.vitrivr.cottontail.core.values.types.RealVectorValue
import org.vitrivr.cottontail.dbms.index.va.signature.EquidistantVAFMarks
import org.vitrivr.cottontail.dbms.index.va.signature.VAFSignature
import kotlin.math.max
import kotlin.math.pow

/**
 * A [Bounds] implementation for Lp distance. Based on [1].
 *
 * References:
 * [1] Weber, R. and Blott, S., 1997. An approximation based data structure for similarity search (No. 9141, p. 416). Technical Report 24, ESPRIT Project HERMES.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class LpBounds(query: RealVectorValue<*>, marks: EquidistantVAFMarks, val p: Int) : Bounds() {
    /** [VAFSignature] for the query [RealVectorValue]. */
    override val query = DoubleArray(query.logicalSize) { query[it].asDouble().value }

    /** [VAFSignature] for the query [RealVectorValue]. */
    override val rq = marks.getSignature(query)

    /** Internal lookup table for pre-calculated values used in bounds calculation. */
    private val lat = Array(marks.marks.size) { j ->
        val qj = query[j].value.toDouble()
        Array(marks.marks[j].size) {
            doubleArrayOf((qj - marks.marks[j][it]).pow(p), (marks.marks[j][it] - qj).pow(p))
        }
    }

    /**
     * Checks if the given [VAFSignature] is a VA-SSA candidate according to [1] by comparing the
     * lower bounds estimation to the given threshold and returns true if so and false otherwise.
     *
     * @param signature The [VAFSignature] to check.
     * @return True if [VAFSignature] is a candidate, false otherwise.
     */
    override fun lb(signature: VAFSignature, threshold: Double): Double {
        var sum = 0.0
        for (i in 0 until signature.size()) {
            val rij = signature[i]
            if (rij < this.rq[i]) {
                sum += this.lat[i][rij + 1][0]
            } else if (rij > this.rq[i]) {
                sum += this.lat[i][rij][1]
            }

        }
        return sum.pow(1.0 / p)
    }

    /**
     * Checks if the given [VAFSignature] is a VA-SSA candidate according to [1] by comparing the
     * lower bounds estimation to the given threshold and returns true if so and false otherwise.
     *
     * @param signature The [VAFSignature] to check.
     * @return True if [VAFSignature] is a candidate, false otherwise.
     */
    override fun ub(signature: VAFSignature): Double {
        var sum = 0.0
        for (i in 0 until signature.size()) {
            val rij = signature[i]
            if (rij < this.rq[i]) {
                sum += this.lat[i][rij][1]
            } else if (rij > this.rq[i]) {
                sum += this.lat[i][rij + 1][0]
            } else {
                sum += max(this.lat[i][rij + 1][0], this.lat[i][rij][1])
            }
        }
        return sum.pow(1.0 / p)
    }
}