package org.vitrivr.cottontail.core.queries.functions

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value

/**
 * A signature that uniquely identifies a [Function].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed interface Signature<T: Argument> {
    /** The [Name.FunctionName]s that makes up this [Signature]. */
    val name: Name.FunctionName

    /** The [Argument]s that make up this [Signature]. */
    val arguments: Array<T>

    /** Returns the arity of this [Signature]. */
    val arity: Int
        get() = this.arguments.size

    /**
     * Generates and returns a [Signature.Open] for this [Signature]. The [Signature.Open] is typically used by a [FunctionGenerator].
     *
     * @return Generator [Signature] for this [Signature]
     */
    fun toOpen() = Open(this.name, this.arguments.map { it.toOpen() }.toTypedArray())

    /**
     * Checks if provided [Signature] is equivalent to this [Signature] and returns true or false respectively.
     *
     * @param other [Signature] to check.
     * @return True on equivalence, false otherwise.
     */
    fun collides(other: Signature<*>): Boolean {
        if (this.name != other.name) return false
        if (this.arity != other.arity) return false
        return this.arguments.mapIndexed { i, a -> a.isCompatible(other.arguments[i]) }.all { it }
    }

    /**
     * A [Signature.Closed] with known [Argument]s and return [Types].
     */
    data class Closed<R: Value>(override val name: Name.FunctionName, override val arguments: Array<Argument.Typed<*>>, val returnType: Types<R>): Signature<Argument.Typed<*>> {
        /** Simplified constructor with [Types] instead of [Argument]s. */
        constructor(name: Name.FunctionName, arguments: Array<Types<*>>, returnType: Types<R>): this(name, arguments.map { Argument.Typed(it) }.toTypedArray(), returnType)
        fun toSemiClosed() = SemiClosed(this.name, this.arguments)
        override fun toString(): String = "$name(${this.arguments.joinToString(",")}): $returnType"
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Closed<*>) return false
            if (this.name != other.name) return false
            if (this.returnType != other.returnType) return false
            if (!this.arguments.contentEquals(other.arguments)) return false
            return true
        }
        override fun hashCode(): Int {
            var result = this.name.hashCode()
            result = 31 * result + this.returnType.hashCode()
            result = 31 * result + this.arguments.contentHashCode()
            return result
        }
    }

    /**
     * A [Signature.Closed] with known [Argument]s and unknown return [Types].
     */
    data class SemiClosed(override val name: Name.FunctionName, override val arguments: Array<Argument.Typed<*>>): Signature<Argument.Typed<*>> {
        override fun toString(): String = "$name(${this.arguments.joinToString(",")}): ?"
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is SemiClosed) return false
            if (this.name != other.name) return false
            if (!this.arguments.contentEquals(other.arguments)) return false
            return true
        }
        override fun hashCode(): Int {
            var result = this.name.hashCode()
            result = 31 * result + this.arguments.contentHashCode()
            return result
        }
    }

    /**
     * A [Signature.Open] that may also contain open [Argument]s and does not contain any return type.
     */
    data class Open(override val name: Name.FunctionName, override val arguments: Array<Argument.Open>): Signature<Argument.Open> {
        override fun toString(): String = "$name(${this.arguments.joinToString(",")}): ?"
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Open) return false
            if (this.name != other.name) return false
            if (!this.arguments.contentEquals(other.arguments)) return false
            return true
        }
        override fun hashCode(): Int {
            var result = name.hashCode()
            result = 31 * result + this.arguments.contentHashCode()
            return result
        }
    }
}
