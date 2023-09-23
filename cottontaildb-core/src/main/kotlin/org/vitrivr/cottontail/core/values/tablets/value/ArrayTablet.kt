package org.vitrivr.cottontail.core.values.tablets.value

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.tablets.Tablet

/**
 * An abstract [Tablet] implementation, that wraps an [Array] of [Value]s of type [T]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ArrayTablet<T: Value>(override val type: Types<T>, internal val values: Array<T?>): Tablet<T> {

    companion object {
        /**
         * Generates and returns a [Tablet] of the provided [size] and [Types]
         *
         * @param size The [size] of the [Tablet]. Must be a power of two.
         * @return [ArrayTablet]
         */
        @Suppress("UNCHECKED_CAST")
        inline fun <reified T: Value> of(size: Int, type: Types<T>): ArrayTablet<T>
           = ArrayTablet(type, Array<Value?>(size) { null } as Array<T?>)
    }

    override val size: Int
        get() = this.values.size

    override fun isNull(index: Int): Boolean = this.values[index] == null

    override fun get(index: Int): T? = this.values[index]

    override fun set(index: Int, value: T?) {
        this.values[index] = value
    }
}