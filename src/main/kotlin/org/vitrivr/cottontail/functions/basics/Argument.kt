package org.vitrivr.cottontail.functions.basics

import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.types.Value

/**
 * An [Argument] used in a [Signature].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface Argument {

    /**
     * Returns true if provided [Argument] is compatible with this [Argument].
     *
     * @param argument The [Argument] to compare this [Argument] to.
     * @return true if compatibility is given, false otherwise.
     */
    fun isCompatible(argument: Argument): Boolean

    /**
     * An [Argument] related to a [Type] that is part of Cottontail DB's type system.
     */
    data class Typed<T: Value>(val type: Type<T>): Argument {
        /**
         * [Typed]s are only compatible to arguments of the same type.
         *
         * @param argument The [Argument] to compare this [Typed] to.
         * @return true if compatibility is given, false otherwise.
         */
        override fun isCompatible(argument: Argument): Boolean = (this == argument)
        override fun toString(): String = "Typed[$type]"
    }

    /**
     * A numeric argument, i.e., an argument that must contain a vector value
     */
    object Numeric: Argument {
        /**
         * [Argument.Vector]s are compatible to all types of [Argument.Vector]s.
         *
         * @param argument The [Argument] to compare this [Open] to.
         * @return true if compatibility is given, false otherwise.
         */
        override fun isCompatible(argument: Argument): Boolean = (argument is Typed<*>) && (argument.type.vector)
        override fun toString(): String = "Open[Numeric]"
    }

    /**
     * A vector argument, i.e., an argument that must contain a vector value
     */
    object Vector: Argument {
        /**
         * [Argument.Vector]s are compatible to all types of [Argument.Vector]s.
         *
         * @param argument The [Argument] to compare this [Open] to.
         * @return true if compatibility is given, false otherwise.
         */
        override fun isCompatible(argument: Argument): Boolean = (argument is Typed<*>) && (argument.type.vector)
        override fun toString(): String = "Open[Vector]"
    }

    /**
     * An open argument, i.e., an argument that is undefined.
     */
    object Open: Argument {

        /**
         * [Open]s are compatible to all types of [Argument]s.
         *
         * @param argument The [Argument] to compare this [Open] to.
         * @return true if compatibility is given, false otherwise.
         */
        override fun isCompatible(argument: Argument): Boolean = true
        override fun toString(): String = "Open[Any]"
    }
}