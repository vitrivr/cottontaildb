package org.vitrivr.cottontail.core.values.types

import com.google.common.hash.Funnel
import com.google.common.hash.PrimitiveSink

/**
 * This is an abstraction over the existing primitive types provided by Kotlin. It allows for the
 * advanced type system implemented by Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.6.0
 */
interface Value : Comparable<Value> {

    /** Size of this [Value]. */
    val logicalSize: Int

    /** The [Types] of this [Value]. */
    val type: Types<*>

    /**
     * Compares two [Value]s. Returns true, if they are equal, and false otherwise.
     *
     * TODO: This method is required because it is currently not possible to override
     * equals() in Kotlin inline classes. Once this changes, this method should be removed.
     *
     * @param other Value to compare to.
     * @return true if equal, false otherwise.
     */
    fun isEqual(other: Value): Boolean

    object ValueFunnel : Funnel<Value> {
        override fun funnel(value: Value, into: PrimitiveSink) {
            into.putInt(value.hashCode())
        }
    }
}