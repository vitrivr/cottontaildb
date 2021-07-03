package org.vitrivr.cottontail.functions.basics

import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A signature that uniquely identifies a [Function].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class Signature<R: Value>(val name: String, val returnType: Type<R>) {
    /** Returns the arity of this [Signature]. */
    abstract val arity: Int

    /**
     * Checks if other [Signature] collides with this [Signature] and returns true or false respectively.
     *
     * @param other [Signature] to check.
     * @return True if collision is expected, false otherwise.
     */
    abstract fun collides(other: Signature<*>): Boolean

    /**
     * A [Signature.Closed] with known arguments.
     */
    class Closed<R: Value>(name: String, returnType: Type<R>, val arguments: Array<Type<*>>): Signature<R>(name, returnType) {
        override val arity: Int
            get() = arguments.size

        /**
         * Converts this [Signature.Closed] to a [Signature.Open].
         *
         * @return [Signature.Open]
         */
        fun toOpen(): Open<R> = Open(this.name, this.returnType, this.arguments.size)

        /**
         * Checks if other [Signature] collides with this [Signature.Closed] and returns true or false respectively.
         *
         * @param other [Signature] to check.
         * @return True if collision is expected, false otherwise.
         */
        override fun collides(other: Signature<*>): Boolean = when(other) {
            is Open -> this.name == other.name && this.returnType == other.returnType && this.arity == other.arity
            is Closed -> (this == other)
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Closed<*>) return false

            if (name != other.name) return false
            if (!arguments.contentEquals(other.arguments)) return false
            if (returnType != other.returnType) return false

            return true
        }

        override fun hashCode(): Int {
            var result = name.hashCode()
            result = 31 * result + arguments.contentHashCode()
            result = 31 * result + returnType.hashCode()
            return result
        }

        override fun toString(): String = "$name(${this.arguments.joinToString(",")}) -> $returnType"
    }

    /**
     * A [Signature.Open] with unknown arguments but known argument arity.
     */
    class Open<R: Value>(name: String, returnType: Type<R>, override val arity: Int): Signature<R>(name, returnType) {

        /**
         * Checks if other [Signature] collides with this [Signature.Open] and returns true or false respectively.
         *
         * @param other [Signature] to check.
         * @return True if collision is expected, false otherwise.
         */
        override fun collides(other: Signature<*>): Boolean = when(other) {
            is Open -> (this == other)
            is Closed -> this.name == other.name && this.returnType == other.returnType && this.arity == other.arity
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Open<*>) return false

            if (name != other.name) return false
            if (arity != other.arity) return false
            if (returnType != other.returnType) return false

            return true
        }

        override fun hashCode(): Int {
            var result = name.hashCode()
            result = 31 * result + arity.hashCode()
            result = 31 * result + returnType.hashCode()
            return result
        }

        override fun toString(): String = "$name(${(0 until this.arity).joinToString(",") { "?" }}) -> $returnType"
    }
}
