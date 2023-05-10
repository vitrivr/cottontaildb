package org.vitrivr.cottontail.core.types

import org.vitrivr.cottontail.core.types.Types

/**
 * This is an abstraction over the existing primitive types provided by Kotlin. It allows for the
 * advanced type system implemented by Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.0.0
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


}