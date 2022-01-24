package org.vitrivr.cottontail.functions.basics

import org.vitrivr.cottontail.model.values.types.Types
import org.vitrivr.cottontail.model.values.types.Value

/**
 * An [Argument] used in a [Signature].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
sealed interface Argument {
    /**
     * Returns true if provided [Value] is compatible with this [Argument].
     *
     * @param value The [Value] to compare this [Argument] to.
     * @return true if compatibility is given, false otherwise.
     */
    fun isCompatible(value: Value?): Boolean

    /**
     * Returns true if provided [Argument] is compatible with this [Argument].
     *
     * @param argument The [Argument] to compare this [Argument] to.
     * @return true if compatibility is given, false otherwise.
     */
    fun isCompatible(argument: Argument): Boolean

    /**
     * An [Argument] related to a [Types] that is part of Cottontail DB's type system.
     */
    data class Typed<T: Value>(val type: Types<T>): Argument {
        /**
         * [Typed]s are only compatible to arguments of the same type.
         *
         * @param argument The [Argument] to compare this [Typed] to.
         * @return true if compatibility is given, false otherwise.
         */
        override fun isCompatible(argument: Argument): Boolean = (this == argument)

        override fun isCompatible(value: Value?): Boolean = (value == null || value.type == this.type)

        override fun toString(): String = "Typed[$type]"
    }

    /**
     * A numeric argument, i.e., an argument that must contain a vector value
     */
    object Numeric: Argument {
        /**
         * [Numeric]s are compatible to all [Value]s that are numbers.
         *
         * @param value The [Value] to compare this [Open] to.
         * @return true if compatibility is given, false otherwise.
         */
        override fun isCompatible(value: Value?): Boolean = (value == null || value.type is Types.Numeric)

        /**
         * [Argument.Vector]s are compatible to all types of [Argument.Vector]s.
         *
         * @param argument The [Argument] to compare this [Open] to.
         * @return true if compatibility is given, false otherwise.
         */
        override fun isCompatible(argument: Argument): Boolean = (argument is Typed<*>) && (argument.type is Types.Vector<*,*>)
        override fun toString(): String = "Open[Numeric]"
    }

    /**
     * A vector argument, i.e., an argument that must contain a vector value
     */
    object Vector: Argument {

        /**
         * [Vector]s are compatible to all [Value]s that are vectors.
         *
         * @param value The [Value] to compare this [Open] to.
         * @return true if compatibility is given, false otherwise.
         */
        override fun isCompatible(value: Value?): Boolean = (value == null || value.type is Types.Vector<*,*>)

        /**
         * [Argument.Vector]s are compatible to all types of [Argument.Vector]s.
         *
         * @param argument The [Argument] to compare this [Open] to.
         * @return true if compatibility is given, false otherwise.
         */
        override fun isCompatible(argument: Argument): Boolean = (argument is Typed<*>) && (argument.type is Types.Vector<*,*>)
        override fun toString(): String = "Open[Vector]"
    }

    /**
     * An open argument, i.e., an argument that is undefined.
     */
    object Open: Argument {

        /**
         * [Open]s are compatible to all types of [Value]s.
         *
         * @param value The [Value] to compare this [Open] to.
         * @return true if compatibility is given, false otherwise.
         */
        override fun isCompatible(value: Value?): Boolean = true

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