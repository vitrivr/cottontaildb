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
 * @version 1.1.0
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
     * Checks if the given [VAFSignature] is a VA-SSA candidate according to [1] by comparing the
     * lower bounds estimation to the given threshold and returns true if so and false otherwise.
     *
     * @param signature The [VAFSignature] to check.
     * @return True if [VAFSignature] is a candidate, false otherwise.
     */
    override fun lb(signature: VAFSignature, threshold: Double): Double {
        val t = threshold.pow(2)
        var sum = 0.0
        for (i in 0 until signature.size()) {
            val rij = signature[i]
            if (rij < this.rq[i]) {
                sum += this.lat[i][rij + 1]
                if (sum > t) break
            } else if (rij > this.rq[i]) {
                sum += this.lat[i][rij]
                if (sum > t) break
            }
        }
        return sqrt(sum)
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
            sum += if (rij < this.rq[i]) {
                this.lat[i][rij]
            } else if (rij > this.rq[i]) {
                this.lat[i][rij + 1]
            } else {
                max(this.lat[i][rij + 1], this.lat[i][rij])
            }
        }
        return sqrt(sum)
    }
}