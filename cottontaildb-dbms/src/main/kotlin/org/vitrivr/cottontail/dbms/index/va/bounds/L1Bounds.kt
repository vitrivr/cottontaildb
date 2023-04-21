package org.vitrivr.cottontail.dbms.index.va.bounds

import org.vitrivr.cottontail.core.values.types.RealVectorValue
import org.vitrivr.cottontail.dbms.index.va.signature.VAFMarks
import org.vitrivr.cottontail.dbms.index.va.signature.VAFSignature
import kotlin.math.absoluteValue
import kotlin.math.max

/**
 * A [Bounds] implementation for L1 (Manhattan) distance. This is equivalent to the implementation
 * in [LpBounds] but more efficient. Based on [1].
 *
 * References:
 * [1] Weber, R. and Blott, S., 1997. An approximation based data structure for similarity search (No. 9141, p. 416). Technical Report 24, ESPRIT Project HERMES.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class L1Bounds(query: RealVectorValue<*>, marks: VAFMarks) : Bounds() {

    /** [VAFSignature] for the query [RealVectorValue]. */
    override val query = DoubleArray(query.logicalSize) { query[it].asDouble().value }

    /** [VAFSignature] for the query [RealVectorValue]. */
    override val rq = marks.getSignature(query)

    /** Internal lookup table for pre-calculated values used in bounds calculation. */
    private val lat = Array(query.logicalSize) { j ->
        val qj = query[j].value.toDouble()
        DoubleArray(marks.marks[j].size) {
            (qj - marks.marks[j][it]).absoluteValue
        }
    }

    /**
     * Calculates and returns the lower bounds for this [L1Bounds].
     *
     * @param signature [VAFSignature] to calculate bounds for.
     * @param threshold Threshold for early abort.
     * @return Lower bound.
     */
    override fun lb(signature: VAFSignature, threshold: Double): Double {
        var sum = 0.0
        for (i in 0 until signature.size()) {
            val rij = signature[i]
            if (rij < this.rq[i]) {
                sum += this.lat[i][rij + 1]
                if (sum > threshold) break
            } else if (rij > this.rq[i]) {
                sum += this.lat[i][rij]
                if (sum > threshold) break
            }
        }
        return sum
    }

    /**
     * Calculates and returns the upper bounds for this [L1Bounds].
     *
     * @param signature [VAFSignature] to calculate bounds for.
     * @param threshold Threshold for early abort.
     * @return Upper bound.
     */
    override fun ub(signature: VAFSignature, threshold: Double): Double {
        var sum = 0.0
        for (i in 0 until signature.size()) {
            val rij = signature[i]
            sum += if (rij < this.rq[i]) {
                this.lat[i][rij]
            } else if (rij > this.rq[i]) {
                this.lat[i][rij + 1]
            } else {
                max(this.lat[i][rij + 1], this.lat[i][rij])
            }
            if (sum > threshold) break
        }
        return sum
    }

    /**
     * Calculates and returns the bounds for this [L1Bounds].
     *
     * @param signature [VAFSignature] to calculate bounds for.
     * @return Bounds
     */
    override fun bounds(signature: VAFSignature): Pair<Double,Double> {
        var lb = 0.0
        var ub = 0.0
        for (i in 0 until signature.size()) {
            val rij = signature[i]
            if (rij < this.rq[i]) {
                lb += this.lat[i][rij + 1]
                ub += this.lat[i][rij]
            } else if (rij > this.rq[i]) {
                lb += this.lat[i][rij]
                ub += this.lat[i][rij + 1]
            } else {
                ub += max(this.lat[i][rij + 1], this.lat[i][rij])
            }
        }
        return Pair(lb, ub)
    }
}