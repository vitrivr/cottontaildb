package org.vitrivr.cottontail.core.functions

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value

/**
 * A signature that uniquely identifies a [Function].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed class Signature<R: Value>(val name: Name.FunctionName, val returnType: Types<R>? = null) {
    /** Returns the arity of this [Signature]. */
    abstract val arity: Int

    /**
     * Checks if provided [Signature] is equivalent to this [Signature] and returns true or false respectively.
     *
     * @param other [Signature] to check.
     * @return True on equivalence, false otherwise.
     */
    abstract fun collides(other: Signature<*>): Boolean

    /**
     * A [Signature.Closed] with known arguments.
     */
    class Closed<R: Value>(name: Name.FunctionName, val arguments: Array<Argument.Typed<*>>, returnType: Types<R>? = null): Signature<R>(name, returnType) {
        override val arity: Int
            get() = this.arguments.size

        /**
         * Converts this [Signature.Closed] to a [Signature.Open].
         *
         * @param retain Array of argument positions that should be retained.
         *
         * @return [Signature.Open]
         */
        fun toOpen(vararg retain: Int): Open<R> = Open(this.name, this.arguments.mapIndexed { i, t -> if (retain.contains(i)) t else Argument.Open }.toTypedArray(), this.returnType)

        /**
         * Checks if other [Signature] collides with this [Signature.Closed] and returns true or false respectively.
         *
         * @param other [Signature] to check.
         * @return True if collision is expected, false otherwise.
         */
        override fun collides(other: Signature<*>): Boolean = when(other) {
            is Open -> other.includes(this)
            is Closed -> this == other
        }

        /**
         * Checks for equality; return type of a [Signature] is not considered for that comparison.
         */
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Closed<*>) return false

            if (this.name != other.name) return false
            if (!this.arguments.contentEquals(other.arguments)) return false

            return true
        }

        /**
         * Generates hash code; the return type of a [Signature] is not considered for hash code.
         */
        override fun hashCode(): Int {
            var result = this.name.hashCode()
            result = 31 * result + this.arguments.contentHashCode()
            return result
        }

        override fun toString(): String = "$name(${this.arguments.joinToString(",")}) -> $returnType"
    }

    /**
     * A [Signature.Open] that may contain [Argument.Open]s.
     */
    class Open<R: Value>(name: Name.FunctionName, val arguments: Array<Argument>, returnType: Types<R>? = null): Signature<R>(name, returnType) {

        /** Returns the [arity] of this [Signature.Open]*/
        override val arity: Int
            get() = this.arguments.size

        /**
         * Checks if other [Signature] collides with this [Signature.Open] and returns true or false respectively.
         *
         * @param other [Signature] to check.
         * @return True if collision is detected, false otherwise.
         */
        override fun collides(other: Signature<*>): Boolean = when(other) {
            is Open -> (this == other)
            is Closed -> this.includes(other)
        }

        /**
         * Checks if this [Signature.Open] includes the provided [Signature.Closed].
         *
         * @param other The [Signature.Closed] that should be checked.
         * @return true if [Signature.Closed] is included, false otherwise.
         */
        fun includes(other: Closed<*>): Boolean {
            if (this.name != other.name) return false
            if (this.arity != other.arity) return false
            return this.arguments.mapIndexed { i, a -> a.isCompatible(other.arguments[i]) }.all { it }
        }

        /**
         * Checks for equality; return type of a [Signature] is not considered for that comparison.
         */
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Open<*>) return false

            if (this.name != other.name) return false
            if (this.arity != other.arity) return false

            return true
        }

        /**
         * Generates hash code; return type of a [Signature] is not considered for hash code.
         */
        override fun hashCode(): Int {
            var result = name.hashCode()
            result = 31 * result + arity.hashCode()
            return result
        }

        override fun toString(): String = "$name(${this.arguments.joinToString(",")}): $returnType"
    }
}
