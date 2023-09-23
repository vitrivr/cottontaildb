package org.vitrivr.cottontail.core.values

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * This is a [Value] abstraction over a [String].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@Serializable
@SerialName("String")
@JvmInline
value class StringValue(override val value: String) : ScalarValue<String>, Value {

    companion object {
        /** The empty [StringValue]. */
        val EMPTY = StringValue("")
    }

    /** The [Types] of this [StringValue]. */
    override val type: Types<*>
        get() = Types.String

    /** The logical size of this [StringValue]. */
    override val logicalSize: Int
        get() = this.value.length

    override val physicalSize: Int
        get() = this.value.length * 8

    /**
     * Compares this [StringValue] to another [Value]. Returns -1, 0 or 1 of other value is smaller,
     * equal or greater than this value. [StringValue] can only be compared to other [StringValue]s.
     *
     * @param other [Value] to compare to.
     * @return -1, 0 or 1 of other value is smaller, equal or greater than this value
     */
    override fun compareTo(other: Value): Int = when (other) {
        is StringValue -> this.value.compareTo(other.value)
        else -> throw IllegalArgumentException("StringValues can only be compared to other StringValues.")
    }

    /**
     * Checks for equality between this [StringValue] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [StringValue] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean
        = (other is StringValue) && (other.value == this.value)

    /**
     * Converts this [StringValue] to a [CottontailGrpc.Literal.Builder] gRCP representation.
     *
     * @return [CottontailGrpc.Literal.Builder]
     */
    override fun toGrpc(): CottontailGrpc.Literal.Builder
        = CottontailGrpc.Literal.newBuilder().setStringData(this.value)

    /**
     * Returns the [value] held by this [StringValue].
     */
    override fun toString(): String = this.value
}