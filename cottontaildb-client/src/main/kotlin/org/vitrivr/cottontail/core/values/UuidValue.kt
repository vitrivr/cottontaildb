package org.vitrivr.cottontail.core.values

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.types.ScalarValue
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc.Uuid
import org.vitrivr.cottontail.serialization.UUIDSerializer
import java.util.*

/**
 * A [PublicValue] that holds a [UUID].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable(with = UUIDSerializer::class)
@SerialName("UUID")
@JvmInline
value class UuidValue(override val value: UUID) : ScalarValue<UUID>, PublicValue {
    /**
     * Constructs a [UuidValue] from a [String].
     *
     * @param string The [String] to construct [UuidValue] from.
     */
    constructor(string: String):  this(UUID.fromString(string))

    /**
     * Constructs a [UuidValue] from two longs.
     *
     * @param mostSigBits The [Long] representing the most significant bits.
     * @param leastSigBits The [Long] representing the least significant bits.
     */
    constructor(mostSigBits: Long, leastSigBits: Long):  this(UUID(mostSigBits, leastSigBits))

    /** The [Types] of this [StringValue]. */
    override val type: Types<*>
        get() = Types.Uuid

    /** The logical size of this [UuidValue] .*/
    override val logicalSize: Int
        get() = this.type.logicalSize

    /** Accessor for most significant bits of [UuidValue]. */
    val mostSignificantBits: Long
        get() = this.value.mostSignificantBits

    /** Accessor for least significant bits of [UuidValue]. */
    val leastSignificantBits: Long
        get() = this.value.leastSignificantBits

    /**
     * Compares this [UuidValue] to another [Value]. Returns -1, 0 or 1 of other value is smaller,
     * equal or greater than this value. [UuidValue] can only be compared to other [UuidValue]s.
     *
     * @param other [Value] to compare to.
     * @return -1, 0 or 1 of other value is smaller, equal or greater than this value
     */
    override fun compareTo(other: Value): Int = when (other) {
        is UuidValue -> this.value.compareTo(other.value)
        else -> throw IllegalArgumentException("UUIDValue can only be compared to other UUIDValues.")
    }

    /**
     * Checks for equality between this [UuidValue] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [UuidValue] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean = (other is UuidValue) && (other.value == this.value)

    /**
     * Converts this [StringValue] to a [CottontailGrpc.Literal] gRCP representation.
     *
     * @return [CottontailGrpc.Literal]
     */
    override fun toGrpc(): CottontailGrpc.Literal = CottontailGrpc.Literal.newBuilder().setUuidData(Uuid.newBuilder().setLeastSignificant(this.value.leastSignificantBits).setMostSignificant(this.value.mostSignificantBits)).build()

    /**
     * Returns the [value] held by this [StringValue].
     */
    override fun toString(): String = this.value.toString()
}