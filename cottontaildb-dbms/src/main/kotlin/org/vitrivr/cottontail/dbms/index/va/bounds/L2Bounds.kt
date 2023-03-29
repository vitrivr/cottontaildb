package org.vitrivr.cottontail.dbms.index.va.bounds

import org.vitrivr.cottontail.core.values.types.RealVectorValue
import org.vitrivr.cottontail.dbms.index.va.signature.EquidistantVAFMarks
import org.vitrivr.cottontail.dbms.index.va.signature.VAFSignature
import kotlin.math.max
import kotlin.math.pow
import kotlin.math.sqrt

/**
 * A [Bounds] implementation for L2 (Euclidean) distance. This is equivalent to the implementation
 * in [LpBounds] but more efficient. Based on [1].
 *
 * References:
 * [1] Weber, R. and Blott, S., 1997. An approximation based data structure for similarity search (No. 9141, p. 416). Technical Report 24, ESPRIT Project HERMES.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class L2Bounds(query: RealVectorValue<*>, marks: EquidistantVAFMarks) : Bounds() {
    /** [VAFSignature] for the query [RealVectorValue]. */
    override val query = DoubleArray(query.logicalSize) { query[it].asDouble().value }

    /** [VAFSignature] for the query [RealVectorValue]. */
    override val rq = marks.getSignature(query)

    /** Internal lookup table for pre-calculated values used in bounds calculation. */
    private val lat = Array(query.logicalSize) { j ->
        val qj = query[j].value.toDouble()
        DoubleArray(marks.marks[j].size) { (qj - marks.marks[j][it]).pow(2) }
    }

    /**
     * Calculates and returns the lower bounds for this [L2Bounds].
     *
     * @param signature The [VAFSignature] to check.
     * @param threshold Threshold for early abort.
     * @return True if [VAFSignature] is a candidate, false otherwise.
     */
    override fun lb(signature: VAFSignature, threshold: Double): Double {
        val t = threshold.pow(2)
        var sum = 0.0
        for (i in 0 until signature.size()) {
            val rij = signature[i]
            val rq = this.rq[i]
            val lat = this.lat[i]
            if (rij < rq) {
                sum += lat[rij + 1]
                if (sum > t) break
            } else if (rij > rq) {
                sum += lat[rij]
                if (sum > t) break
            }
        }
        return sqrt(sum)
    }

    /**
     * Calculates and returns the lower bounds for this [L2Bounds].
     *
     * @param signature The [VAFSignature] to check.
     * @param threshold Threshold for early abort.
     * @return True if [VAFSignature] is a candidate, false otherwise.
     */
    override fun ub(signature: VAFSignature, threshold: Double): Double {
        val t = threshold.pow(2)
        var sum = 0.0
        for (i in 0 until signature.size()) {
            val rij = signature[i]
            val rq = this.rq[i]
            val lat = this.lat[i]
            sum += if (rij < rq) {
                lat[rij]
            } else if (rij > rq) {
                lat[rij + 1]
            } else {
                max(lat[rij + 1], lat[rij])
            }
            if (sum > t) break
        }
        return sqrt(sum)
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
            val rq = this.rq[i]
            val lat = this.lat[i]
            if (rij < rq) {
                lb += lat[rij + 1]
                ub += lat[rij]
            } else if (rij > rq) {
                lb += lat[rij]
                ub += lat[rij + 1]
            } else {
                ub += max(lat[rij + 1], lat[rij])
            }
        }
        return Pair(sqrt(lb), sqrt(ub))
    }
}