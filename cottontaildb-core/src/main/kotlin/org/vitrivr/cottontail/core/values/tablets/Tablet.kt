package org.vitrivr.cottontail.core.values.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.*

/**
 * A [Tablet] of [Value]s. Used to group [Value]s, so that they can be processed and serialized in batches.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface Tablet<T: Value> {
    /** The size of this [Tablet], i.e., the number of elements. */
    val size: kotlin.Int

    /** The [Types] of the [Value] held by this [Tablet]. */
    val type: Types<T>

    /**
     * Checks if the value at position [index] is set (or not).
     *
     * @param index The index to check.
     * @return True if index is set, false otherwise.
     */
    fun isNull(index: kotlin.Int): kotlin.Boolean

    /**
     * Gets and returns a [Value] held in this [Tablet].
     *
     * @param index The index of the [Value] to return.
     * @return [Value] or null
     */
    operator fun get(index: kotlin.Int): T?

    /**
     * Set a [Value] in this [Tablet].
     *
     * @param index The index of the [Value] to set.
     * @param value The new [Value] or null
     */
    operator fun set(index: kotlin.Int, value: T?)

    /**
     * A [Tablet] for [BooleanValue]s
     */
    interface Boolean: Tablet<BooleanValue>

    /**
     * A [Tablet] for [ByteValue]s
     */
    interface Byte: Tablet<ByteValue>

    /**
     * A [Tablet] for [ShortValue]s
     */
    interface Short: Tablet<ShortValue>

    /**
     * A [Tablet] for [IntValue]s
     */
    interface Int: Tablet<IntValue>

    /**
     * A [Tablet] for [LongValue]s
     */
    interface Long: Tablet<LongValue>

    /**
     * A [Tablet] for [FloatValue]s
     */
    interface Float: Tablet<FloatValue>

    /**
     * A [Tablet] for [DoubleValue]s
     */
    interface Double: Tablet<DoubleValue>

    /**
     * A [Tablet] for [DateValue]s
     */
    interface Date: Tablet<DateValue>

    /**
     * A [Tablet] for [UuidValue]s
     */
    interface Uuid: Tablet<UuidValue>

    /**
     * A [Tablet] for [Complex32Value]s
     */
    interface Complex32: Tablet<Complex32Value>

    /**
     * A [Tablet] for [Complex64Value]s
     */
    interface Complex64: Tablet<Complex64Value>

    /**
     * A [Tablet] for [BooleanVectorValue]s
     */
    interface BooleanVector: Tablet<BooleanVectorValue>

    /**
     * A [Tablet] for [IntVectorValue]s
     */
    interface IntVector: Tablet<IntVectorValue>

    /**
     * A [Tablet] for [LongVectorValue]s
     */
    interface LongVector: Tablet<LongVectorValue>

    /**
     * A [Tablet] for [FloatVectorValue]s
     */
    interface FloatVector: Tablet<FloatVectorValue>

    /**
     * A [Tablet] for [DoubleVectorValue]s
     */
    interface DoubleVector: Tablet<DoubleVectorValue>

    /**
     * A [Tablet] for [Complex32VectorValue]s
     */
    interface Complex32Vector: Tablet<Complex32VectorValue>

    /**
     * A [Tablet] for [Complex64VectorValue]s
     */
    interface Complex64Vector: Tablet<Complex64VectorValue>
}