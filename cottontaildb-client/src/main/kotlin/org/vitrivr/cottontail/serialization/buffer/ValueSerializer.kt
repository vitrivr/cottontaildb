package org.vitrivr.cottontail.serialization.buffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import java.nio.ByteBuffer

/**
 * A serializer for [Value] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface ValueSerializer<T: Value> {

    companion object {
        /**
         * Returns the [ValueSerializer] for this [Types].
         *
         * @return [ValueSerializer]
         */
        fun <T: Value> serializer(type: Types<T>) = when(type) {
            Types.Boolean -> BooleanValueSerializer
            Types.Date -> DateValueSerializer
            Types.Byte -> ByteValueSerializer
            Types.Complex32 -> Complex32ValueSerializer
            Types.Complex64 -> Complex64ValueSerializer
            Types.Double -> DoubleValueSerializer
            Types.Float -> FloatValueSerializer
            Types.Int -> IntValueSerializer
            Types.Long -> LongValueSerializer
            Types.Short -> ShortValueSerializer
            Types.String -> StringValueSerializer
            Types.Uuid -> UuidValueSerializer
            is Types.BooleanVector -> BooleanVectorValueSerializer(type.logicalSize)
            is Types.Complex32Vector -> Complex32VectorValueSerializer(type.logicalSize)
            is Types.Complex64Vector -> Complex64VectorValueSerializer(type.logicalSize)
            is Types.DoubleVector -> DoubleVectorValueSerializer(type.logicalSize)
            is Types.FloatVector -> FloatVectorValueSerializer(type.logicalSize)
            is Types.IntVector -> IntVectorValueSerializer(type.logicalSize)
            is Types.LongVector -> LongVectorValueSerializer(type.logicalSize)
            is Types.ByteString -> ByteStringValueSerializer
            is Types.ShortVector -> ShortVectorValueSerializer(type.logicalSize)
            is Types.HalfVector -> HalfVectorValueSerializer(type.logicalSize)
        }
    }


    /** The [Types] converted by this [ValueSerializer]. */
    val type: Types<T>

    /**
     * Converts a [ByteBuffer] to a [Value] of type [T].
     *
     * @param buffer The [ByteBuffer] to convert.
     * @return The resulting [Value].
     */
    fun fromBuffer(buffer: ByteBuffer): T

    /**
     * Converts a [Value] of type [T] to a [ByteBuffer].
     */
    fun toBuffer(value: T): ByteBuffer
}