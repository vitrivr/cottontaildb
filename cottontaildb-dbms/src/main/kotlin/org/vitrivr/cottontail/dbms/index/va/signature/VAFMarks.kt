package org.vitrivr.cottontail.dbms.index.va.signature

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.DoubleBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.types.RealVectorValue
import java.io.ByteArrayInputStream

/**
 * Double precision [VAFMarks] implementation used in [VAFIndex] structures.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.1.0
 */
@JvmInline
value class VAFMarks(val marks: Array<DoubleArray>): Comparable<VAFMarks> {

    companion object: ComparableBinding() {
        /**
         * Calculates and returns equidistant [VAFMarks]
         *
         * @param min [DoubleArray] containing the per-dimension minima.
         * @param max [DoubleArray] containing the per-dimension maxima.
         * @param marksPerDimension The number of marks per dimension.
         */
        fun getEquidistantMarks(min: DoubleArray, max: DoubleArray, marksPerDimension: IntArray): VAFMarks = VAFMarks(Array(min.size) { i ->
            require(marksPerDimension[i] > 2) { "Need to request more than 2 mark per dimension! (Faulty dimension: $i)" }
            val a = DoubleArray(marksPerDimension[i]) {
                min[i] + it * (max[i] - min[i]) / (marksPerDimension[i] - 1)
            }
            /* Subtract and add small amount Epsilon to ensure min is included to avoid problems with FP approximations. */
            a[0] -= 1e-9
            a[a.lastIndex] += 1e-9
            a
        })

        /**
         * [ComparableBinding] implementation.
         */
        override fun readObject(stream: ByteArrayInputStream)= VAFMarks(Array(IntegerBinding.readCompressed(stream)) { i ->
            DoubleArray(IntegerBinding.readCompressed(stream)) { j ->
                DoubleBinding.BINDING.readObject(stream)
            }
        })

        /**
         * [ComparableBinding] implementation.
         */
        override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
            require(`object` is VAFMarks) { }
            IntegerBinding.writeCompressed(output,  `object`.marks.size)
            for (d in `object`.marks) {
                IntegerBinding.writeCompressed(output, d.size)
                for (v in d) {
                    DoubleBinding.BINDING.writeObject(output, v)
                }
            }
        }
    }

    /** The dimensionality of this [VAFMarks] object. */
    val d: Int
        get() = this.marks.size

    /**
     * This method calculates the signature of a [RealVectorValue]. It checks for every mark if the
     * corresponding vector is beyond that mark or not. If so, the preceding mark is the corresponding mark.
     *
     * Note that this method can return -1, which means that the component is smaller than the smallest mark!
     * This can arise, e.g., if marks are not generated from entire dataset, but just a sampled subset thereof!
     *
     * @param vector The [RealVectorValue] to calculate the cells for.
     * @return An [VAFSignature] containing the signature of the vector.
     */
    fun getSignature(vector: RealVectorValue<*>): VAFSignature = VAFSignature(IntArray(vector.logicalSize) {
        val index = this.marks[it].indexOfFirst { i -> i > vector[it].value.toDouble() }
        if (index == -1) { // all marks are less or equal than the vector component! last mark is what we're looking for!
            (this.marks[it].size - 1)
        } else {
            (index - 1)
        }
    })

    /**
     * Compares this [VAFMarks] to another [VAFMarks].
     *
     * @param other The [VAFMarks] object to compare this [VAFMarks] object to.
     * @return [Int]
     */
    override fun compareTo(other: VAFMarks): Int {
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