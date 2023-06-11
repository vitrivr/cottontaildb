package org.vitrivr.cottontail.core.values

import com.google.protobuf.ByteString
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.types.ScalarValue
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.util.*

/**
 * This is an abstraction over a [ByteArray] (BLOB).
 *
 * @author Luca Rossetto & Ralph Gasser
 * @version 2.0.1
 */
@Serializable
@SerialName("ByteString")
@JvmInline
value class ByteStringValue(override val value: ByteArray) : ScalarValue<ByteArray>, PublicValue {

    companion object {
        val EMPTY = ByteStringValue(ByteArray(0))

        /**
         * Decodes a Base64 string into a [ByteStringValue].
         *
         * @param string The [String] to decode.
         * @return The [ByteStringValue]
         */
        fun fromBase64(string: String) = ByteStringValue(Base64.getDecoder().decode(string))
    }

    override val logicalSize: Int
        get() = this.value.size

    override val type: Types<*>
        get() = Types.ByteString

    /**
     * Compares this [ByteStringValue] to another [Value]. Returns -1, 0 or 1 of other value is smaller,
     * equal or greater than this value. [ByteStringValue] can only be compared to other [ByteStringValue]s.
     *
     * @param other Value to compare to.
     * @return -1, 0 or 1 of other value is smaller, equal or greater than this value
     */
    override fun compareTo(other: Value): Int = if (other is ByteStringValue) {
        Arrays.compare(this.value, other.value)
    } else {
        throw IllegalArgumentException("ByteStringValues can only be compared to other ByteStringValues.")
    }

    /**
     * Checks for equality between this [ByteStringValue] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [ByteStringValue] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean = (other is ByteStringValue) && other.value.contentEquals(this.value)


    /**
     * Returns a Base 64 encoded [String] representation of this [ByteArray].
     *
     * @return [String]
     */
    fun toBase64() = Base64.getEncoder().encodeToString(this.value)

    /**
     * Converts this [ByteValue] to a [CottontailGrpc.Literal] gRCP representation.
     *
     * @return [CottontailGrpc.Literal]
     */
    override fun toGrpc(): CottontailGrpc.Literal
        = CottontailGrpc.Literal.newBuilder().setByteStringData(ByteString.copyFrom(this.value)).build()
}