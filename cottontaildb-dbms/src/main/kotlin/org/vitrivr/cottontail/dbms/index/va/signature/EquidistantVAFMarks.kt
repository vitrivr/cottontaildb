package org.vitrivr.cottontail.dbms.index.va.signature

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.types.RealVectorValue
import org.vitrivr.cottontail.dbms.index.va.VAFIndex
import java.io.ByteArrayInputStream
import kotlin.math.absoluteValue
import kotlin.math.floor
import kotlin.math.min

/**
 * Double precision [EquidistantVAFMarks] implementation used in [VAFIndex] structures.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.2.0
 */
class EquidistantVAFMarks(override val marks: Array<DoubleArray>): VAFMarks, Comparable<EquidistantVAFMarks> {

    /**
     * A [ComparableBinding] to serialize and deserialize [EquidistantVAFMarks].
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    object Binding: ComparableBinding() {
        /**
         * [ComparableBinding] implementation.
         */
        override fun readObject(stream: ByteArrayInputStream): EquidistantVAFMarks {
            val d = IntegerBinding.readCompressed(stream)
            val marksPerDimension = IntegerBinding.readCompressed(stream)
            return EquidistantVAFMarks(Array(d) { DoubleArray(marksPerDimension) { SignedDoubleBinding.BINDING.readObject(stream) } })
        }

        /**
         * [ComparableBinding] implementation.
         */
        override fun writeObject(output: LightOutputStream, `object`: Comparable<EquidistantVAFMarks>) {
            require(`object` is EquidistantVAFMarks) { "VAFMarks.Binding can only be used to serialize instances of VAFMarks." }
            IntegerBinding.writeCompressed(output, `object`.marks.size)
            IntegerBinding.writeCompressed(output, `object`.marksPerDimension)
            for (d in `object`.marks) {
                for (v in d) {
                    SignedDoubleBinding.BINDING.writeObject(output, v)
                }
            }
        }
    }

    /**
     * Constructs [EquidistantVAFMarks] from the given [min] and [max] [DoubleArray].
     *
     * @param min [DoubleArray] containing the per-dimension minima.
     * @param max [DoubleArray] containing the per-dimension maxima.
     * @param marksPerDimension The number of marks per dimension.
     */
    constructor(min: DoubleArray, max: DoubleArray, marksPerDimension: Int): this(Array(min.size) { i ->
        require(marksPerDimension <= Byte.MAX_VALUE) { "Number of marks per " }
        require(marksPerDimension > 3) { "Need to request more than two mark per dimension! (Faulty dimension: $i)" }
        val stepSize = (max[i] - min[i]) / (marksPerDimension - 3)
        DoubleArray(marksPerDimension) { min[i] - stepSize + it * stepSize }
    })

    /** */
    override val minimum: DoubleArray by lazy { DoubleArray(this.d) { this.marks[it].first() } }

    /** */
    override val maximum: DoubleArray by lazy { DoubleArray(this.d) { this.marks[it].last() } }

    /** */
    private val stepSize: DoubleArray by lazy { DoubleArray(this.d) { (this.maximum[it] - this.minimum[it]) / (this.marksPerDimension - 3) } }

    /** The dimensionality of this [EquidistantVAFMarks] object. */
    val d: Int = this.marks.size

    /** The number if marks per dimension, which is a fixed value for [EquidistantVAFMarks] */
    val marksPerDimension: Int = this.marks.first().size

    /** The total number of marks encoded by this [EquidistantVAFMarks]. */
    override val numberOfMarks: Int = this.d * this.marksPerDimension

    /**
     * Returns the number of marks for dimension [d]
     */
    override fun marksForDimension(d: Int) = this.marksPerDimension

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
    override fun getSignature(vector: RealVectorValue<*>): VAFSignature {
        return VAFSignature(ByteArray(vector.logicalSize) { j ->
            val value = vector[j].value.toDouble()
            val index = floor((value + this.minimum[j].absoluteValue) / this.stepSize[j]).toInt() + 1
            min(index, this.marks[0].size - 1).toByte()
        })
    }

    /**
     * Compares this [EquidistantVAFMarks] to another [EquidistantVAFMarks].
     *
     * @param other The [EquidistantVAFMarks] object to compare this [EquidistantVAFMarks] object to.
     * @return [Int]
     */
    override fun compareTo(other: EquidistantVAFMarks): Int {
        for ((i,d) in this.marks.withIndex()) {
            if (i >= other.marks.size) return Int.MIN_VALUE
            for ((j,v) in d.withIndex()) {
                if (j >= other.marks[i].size) return Int.MIN_VALUE
                val comp = v.compareTo(other.marks[i][j])
                if (comp != 0) return comp
            }
        }
        return 0
    }
}