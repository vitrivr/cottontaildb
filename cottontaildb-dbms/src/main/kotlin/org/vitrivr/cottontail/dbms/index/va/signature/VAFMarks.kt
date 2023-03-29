package org.vitrivr.cottontail.dbms.index.va.signature

import org.vitrivr.cottontail.core.values.types.RealVectorValue
import org.vitrivr.cottontail.dbms.index.va.VAFIndex

/**
 * [VAFMarks] implementation used in [VAFIndex] structures.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.2.0
 */
interface VAFMarks {

    /** The array holding the actual marks (or partition points) for every component o */
    val marks: Array<DoubleArray>

    /** The smallest entry, i.e., the first entry held by this [VAFMarks]. Usually, the first entry in [marks]. */
    val minimum: DoubleArray

    /** The smallest entry, i.e., the maximum held by this [EquidistantVAFMarks]. Usually, the last entry in [marks]. */
    val maximum: DoubleArray

    /** The dimensionality of the vector space covered by these [VAFMarks]. */
    val dimension: Int
        get() = this.marks.size

    /** The total number of marks across all dimensions. Can be relevant for [VAFMarks] that don't exhibit an equidistant distribution. */
    val numberOfMarks: Int

    /**
     * Returns the number of marks for component [d]
     *
     * @param d The component to get the number if marks for.
     */
    fun marksForDimension(d: Int) = this.marks[d].size

    /**
     * This method calculates the [VAFSignature] of a [RealVectorValue]. It checks for every mark if the
     * corresponding vector-component falls with in the cell spanned by those marks. If so, it is
     * associated with the preceding cell.
     *
     * Note that this method can return -1, which means that the component is smaller than the smallest mark!
     * This can happen, e.g., if marks are not generated from the entire dataset, but just a sampled subset thereof!
     *
     * @param vector The [RealVectorValue] to calculate the cells for.
     * @return An [VAFSignature] containing the signature of the vector.
     */
    fun getSignature(vector: RealVectorValue<*>): VAFSignature = VAFSignature(ByteArray(vector.logicalSize) { j ->
        val value = vector[j].value.toDouble()
        val marks = this.marks[j]
        for (i in 0 until marks.size - 1) {
            if (marks[i] <= value && value <= marks[i + 1]) {
                return@ByteArray i.toByte()
            }
        }
        (marks.size - 1).toByte()
    })
}
