package org.vitrivr.cottontail.dbms.index.va.signature

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.types.RealVectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexStructCatalogueEntry
import org.vitrivr.cottontail.dbms.index.va.VAFIndex
import org.vitrivr.cottontail.dbms.statistics.columns.*
import java.io.ByteArrayInputStream

/**
 * Double precision [EquidistantVAFMarks] implementation used in [VAFIndex] structures.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.3.0
 */
class EquidistantVAFMarks(override val marks: Array<DoubleArray>): VAFMarks, IndexStructCatalogueEntry() {

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
     * Constructs [EquidistantVAFMarks] from the [VectorValueStatistics].
     *
     * @param statistics [VectorValueStatistics<*>] to construct [EquidistantVAFMarks] for.
     * @param marksPerDimension The number of marks per dimension.
     */
    constructor(statistics: VectorValueStatistics<*>, marksPerDimension: Int): this(
        when (statistics) {
            is FloatVectorValueStatistics -> DoubleArray(statistics.type.logicalSize) { statistics.min.data[it].toDouble() }
            is DoubleVectorValueStatistics -> DoubleArray(statistics.type.logicalSize) {  statistics.min.data[it] }
            is IntVectorValueStatistics -> DoubleArray(statistics.type.logicalSize) { statistics.min.data[it].toDouble() }
            is LongVectorValueStatistics -> DoubleArray(statistics.type.logicalSize) { statistics.min.data[it].toDouble() }
        },
        when (statistics) {
            is FloatVectorValueStatistics -> DoubleArray(statistics.type.logicalSize) { statistics.max.data[it].toDouble() }
            is DoubleVectorValueStatistics -> DoubleArray(statistics.type.logicalSize) {  statistics.max.data[it] }
            is IntVectorValueStatistics -> DoubleArray(statistics.type.logicalSize) { statistics.max.data[it].toDouble() }
            is LongVectorValueStatistics -> DoubleArray(statistics.type.logicalSize) { statistics.max.data[it].toDouble() }
        },
        marksPerDimension
    )

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
        return VAFSignature(ByteArray(vector.logicalSize) { i ->
            val value = vector[i].value.toDouble()
            for (j in 0 until this.marksPerDimension - 1) {
                if (value >= this.marks[i][j] && value <= this.marks[i][j+1]) return@ByteArray j.toByte()
            }
            return VAFSignature.INVALID
        })
    }

    /**
     * Compares this [EquidistantVAFMarks] to another [EquidistantVAFMarks].
     *
     * @param other The [EquidistantVAFMarks] object to compare this [EquidistantVAFMarks] object to.
     * @return [Int]
     */
    override fun compareTo(other: IndexStructCatalogueEntry): Int {
        if (other !is EquidistantVAFMarks) return -1
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