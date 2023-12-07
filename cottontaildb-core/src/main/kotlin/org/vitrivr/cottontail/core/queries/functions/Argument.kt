package org.vitrivr.cottontail.core.queries.functions

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value

/**
 * An [Argument] used in a [Signature].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
sealed interface Argument {
    /**
     * Returns true if provided [Argument] is compatible with this [Argument].
     *
     * @param argument The [Argument] to compare this [Argument] to.
     * @return true if [Argument] is compatible, false otherwise.
     */
    fun isCompatible(argument: Argument): Boolean

    /**
     * Returns true if provided [Value] is compatible with this [Argument].
     *
     * @param value The [Value] to compare this [Argument] to.
     * @return true if [Argument] is compatible, false otherwise.
     */
    fun isCompatible(value: Value?): Boolean

    /**
     * Generalizes this [Argument.Typed] to an either [Argument.Open].
     *
     * @return [Argument.Open] for this [Argument.Typed]
     */
    fun toOpen(): Open

    /**
     * An [Argument] related to a [Types] that is part of Cottontail DB's type system.
     */
    data class Typed<T: Value>(val type: Types<T>): Argument {
        /**
         * [Typed]s are only compatible to [Argument]s of the same type.
         *
         * @param argument The [Argument] to compare this [Typed] to.
         * @return true if [Argument] is compatible, false otherwise.
         */
        override fun isCompatible(argument: Argument): Boolean = (this == argument)

        /**
         * [Typed]s are only compatible to [Value]s of the same type.
         *
         * @param value The [Value] to compare this [Typed] to.
         * @return true if [Value] is compatible, false otherwise.
         */
        override fun isCompatible(value: Value?): Boolean = (value == null || value.type == this.type)

        /**
         * Generalizes this [Argument.Typed] to an either [Argument.Open].
         *
         * @return [Argument.Open] for this [Argument.Typed]
         */
        override fun toOpen(): Open = when(this.type) {
            is Types.Vector<*,*> -> Vector
            is Types.Numeric<*> -> Numeric
            is Types.Scalar -> Scalar
        }

        override fun toString(): String = "Typed[$type]"
    }

    /**
     * An [Open] argument.
     */
    sealed interface Open: Argument {
        override fun toOpen(): Open = this
    }

    /**
     * A scalar argument, i.e., an argument that is not a vector.
     */
    object Scalar: Open {
        /**
         * [Argument.Vector]s are compatible to all types of [Argument.Scalar]s.
         *
         * @param argument The [Argument] to compare this [Argument.Scalar] to.
         * @return true if [Argument] is compatible, false otherwise.
         */
        override fun isCompatible(argument: Argument): Boolean = (argument is Typed<*> && argument.type is Types.Scalar<*>)

        /**
         * [Argument.Scalar]s are compatible to all [Value]s of [Types.Scalar].
         *
         * @param value The [Value] to compare this [Scalar] to.
         * @return true if [Value] is compatible, false otherwise.
         */
        override fun isCompatible(value: Value?): Boolean = (value == null || value.type is Types.Scalar)

        override fun toString(): String = "Open[Scalar]"
    }

    /**
     * A numeric argument, i.e., an argument that must contain a vector value
     */
    object Numeric: Open {
        /**
         * [Argument.Numeric]s are compatible to all types of [Argument.Numeric]s.
         *
         * @param argument The [Argument] to compare this [Numeric] to.
         * @return true if [Argument] is compatible, false otherwise.
         */
        override fun isCompatible(argument: Argument): Boolean = (argument is Typed<*> && argument.type is Types.Numeric<*>)

        /**
         * [Argument.Numeric]s are compatible to all [Value]s of [Types.Numeric].
         *
         * @param value The [Value] to compare this [Argument.Numeric] to.
         * @return true if [Value] is compatible, false otherwise.
         */
        override fun isCompatible(value: Value?): Boolean = (value == null || value.type is Types.Numeric)

        override fun toString(): String = "Open[Numeric]"
    }

    /**
     * A vector argument, i.e., an argument that must contain a vector value
     */
    object Vector: Open {
        /**
         * [Argument.Vector]s are compatible to all types of [Argument.Vector]s.
         *
         * @param argument The [Argument] to compare this [Vector] to.
         * @return true if [Argument] is compatible, false otherwise.
         */
        override fun isCompatible(argument: Argument): Boolean = (argument is Typed<*> && argument.type is Types.Vector<*,*>)

        /**
         * [Argument.Numeric]s are compatible to all [Value]s that are vectors.
         *
         * @param value The [Value] to compare this [Vector] to.
         * @return true if [Value] is compatible, false otherwise.
         */
        override fun isCompatible(value: Value?): Boolean = (value == null || value.type is Types.Vector<*,*>)


        override fun toString(): String = "Open[Vector]"
    }
}