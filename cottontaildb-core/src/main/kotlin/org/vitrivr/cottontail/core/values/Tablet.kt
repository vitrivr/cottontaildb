package org.vitrivr.cottontail.core.values

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value

/**
 * A [Tablet] of [Value]. Used to group [Value]s, that are processed or serialized together.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Tablet<T: Value>(val type: Types<T>, private val values: Array<Value?>) {

    /** The size of this [Tablet], i.e., the number of [Value]s it holds. */
    val size: Int
        get() = this.values.size

    /**
     * Gets and returns a [Value] held in this [Tablet].
     *
     * @param index The index of the [Value] to return.
     * @return [Value] or null
     */
    @Suppress("UNCHECKED_CAST")
    operator fun get(index: Int): T? = this.values[index] as T?

    /**
     * Set a [Value] in this [Tablet].
     *
     * @param index The index of the [Value] to set.
     * @param value The new [Value] or null
     */
    operator fun set(index: Int, value: T?) {
        this.values[index] = value
    }
}