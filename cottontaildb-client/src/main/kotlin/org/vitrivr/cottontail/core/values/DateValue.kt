package org.vitrivr.cottontail.core.values

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.types.ScalarValue
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.time.Instant
import java.util.*

/**
 * This is an abstraction over a [Date].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@Serializable
@SerialName("Date")
@JvmInline
value class DateValue(override val value: Long) : ScalarValue<Long>, PublicValue {

    /**
     * Converts a [Date] to a [DateValue].
     *
     * @param date The [Date] to convert.
     */
    constructor(date: Date) : this(date.time)

    /** The logical size of this [DateValue]. */
    override val logicalSize: Int
        get() = 1

    /** The [Types] of this [DateValue]. */
    override val type: Types<*>
        get() = Types.Date

    /**
     * Compares this [LongValue] to another [Value]. Returns -1, 0 or 1 of other value is smaller,
     * equal or greater than this value. [LongValue] can only be compared to other [NumericValue]s.
     *
     * @param other Value to compare to.
     * @return -1, 0 or 1 of other value is smaller, equal or greater than this value
     */
    override fun compareTo(other: Value): Int = when (other) {
        is DateValue -> this.value.compareTo(other.value)
        else -> throw IllegalArgumentException("DateValue can only be compared to other DateValues.")
    }

    /**
     * Checks for equality between this [DateValue] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [DateValue] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean =
        (other is DateValue) && (other.value == this.value)

    /**
     * Converts this [DateValue] to a [Date] and returns it.
     *
     * @return [Date] representation.
     */
    fun toDate() = Date.from(Instant.ofEpochMilli(this.value))

    /**
     * Converts this [DateValue] to a  [CottontailGrpc.Literal] gRCP representation.
     *
     * @return [CottontailGrpc.Literal]
     */
    override fun toGrpc(): CottontailGrpc.Literal
        = CottontailGrpc.Literal.newBuilder().setDateData(this.value).build()
}