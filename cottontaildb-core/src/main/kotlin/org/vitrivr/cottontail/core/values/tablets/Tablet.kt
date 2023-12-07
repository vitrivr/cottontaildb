package org.vitrivr.cottontail.core.values.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import java.nio.ByteBuffer

/**
 * A [Tablet] of [Value]s. Used to group [Value]s, so that they can be processed and serialized in batches.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface Tablet<T: Value> {
    companion object {

        /**
         * Generates and returns a [Tablet] of the provided [size] and [Types]
         *
         * @param size The [size] of the [Tablet]. Must be a power of two.
         * @param types The [Types] held by the [Tablet].
         * @param direct Flag indicating, whether a direct [ByteBuffer] should be used.
         * @return [Tablet]
         */
        @Suppress("UNCHECKED_CAST")
        fun <T: Value> of (size: Int, types: Types<T>, direct: Boolean = false): Tablet<T> = when(types) {
            Types.Boolean -> BooleanTablet(size, direct)
            Types.Date -> DateTablet(size, direct)
            Types.Byte -> ByteTablet(size, direct)
            Types.Complex32 -> Complex32Tablet(size, direct)
            Types.Complex64 -> Complex64Tablet(size, direct)
            Types.Double -> DoubleTablet(size, direct)
            Types.Float -> FloatTablet(size, direct)
            Types.Int -> IntTablet(size, direct)
            Types.Long -> LongTablet(size, direct)
            Types.Short -> ShortTablet(size, direct)
            Types.Uuid -> UuidTablet(size, direct)
            is Types.BooleanVector -> BooleanVectorTablet(size, types.logicalSize, direct)
            is Types.Complex32Vector -> Complex32VectorTablet(size, types.logicalSize, direct)
            is Types.Complex64Vector -> Complex64VectorTablet(size, types.logicalSize, direct)
            is Types.DoubleVector -> DoubleVectorTablet(size, types.logicalSize, direct)
            is Types.FloatVector ->  FloatVectorTablet(size, types.logicalSize, direct)
            is Types.IntVector ->  IntVectorTablet(size, types.logicalSize, direct)
            is Types.LongVector ->  LongVectorTablet(size, types.logicalSize, direct)
            else -> throw UnsupportedOperationException("The type $types cannot be represented in a tablet.")
        } as Tablet<T>
    }

    /** The size of this [Tablet], i.e., the number of elements. */
    val size: Int

    /** The [Types] of the [Value] held by this [Tablet]. */
    val type: Types<T>

    /** The raw [ByteBuffer] backing this [Tablet]. */
    val buffer: ByteBuffer

    /**
     * Checks if the value at position [index] is set (or not).
     *
     * @param index The index to check.
     * @return True if index is set, false otherwise.
     */
    fun isNull(index: Int): Boolean

    /**
     * Gets and returns a [Value] held in this [Tablet].
     *
     * @param index The index of the [Value] to return.
     * @return [Value] or null
     */
    operator fun get(index: Int): T?

    /**
     * Set a [Value] in this [Tablet].
     *
     * @param index The index of the [Value] to set.
     * @param value The new [Value] or null
     */
    operator fun set(index: Int, value: T?)
}