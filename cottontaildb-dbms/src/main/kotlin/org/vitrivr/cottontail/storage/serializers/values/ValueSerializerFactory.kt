package org.vitrivr.cottontail.storage.serializers.values

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.storage.serializers.values.mapdb.*
import org.vitrivr.cottontail.storage.serializers.values.xodus.*

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializer implementations for its different storage engines.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
object ValueSerializerFactory {
    /**
     * Returns the [MapDBSerializer] for the given [Types].
     *
     * @param type The [Types] to lookup a [MapDBSerializer] for.
     * @return [MapDBSerializer]
     */
    @Suppress("UNCHECKED_CAST")
    fun <T : Value> mapdb(type: Types<T>): MapDBSerializer<T> = when (type) {
        Types.Boolean -> BooleanValueMapDBSerializer
        Types.Date -> DateValueMapDBSerializer
        Types.Byte -> ByteValueMapDBSerializer
        Types.Complex32 -> Complex32ValueMapDBSerializer
        Types.Complex64 -> Complex64ValueMapDBSerializer
        Types.Double -> DoubleValueMapDBSerializer
        Types.Float -> FloatValueMapDBSerializer
        Types.Int -> IntValueMapDBSerializer
        Types.Long -> LongValueMapDBSerializer
        Types.Short -> ShortValueMapDBSerializer
        Types.String -> StringValueMapDBSerializer
        is Types.BooleanVector -> BooleanVectorValueMapDBSerializer(type.logicalSize)
        is Types.Complex32Vector -> Complex32VectorValueMapDBSerializer(type.logicalSize)
        is Types.Complex64Vector -> Complex64VectorValueMapDBSerializer(type.logicalSize)
        is Types.DoubleVector -> DoubleVectorMapDBSerializer(type.logicalSize)
        is Types.FloatVector -> FloatVectorMapDBValueSerializer(type.logicalSize)
        is Types.IntVector -> IntVectorValueMapDBSerializer(type.logicalSize)
        is Types.LongVector -> LongVectorValueMapDBSerializer(type.logicalSize)
        is Types.ByteString -> throw UnsupportedOperationException("ByteString not supported by MapDB storage")
    } as MapDBSerializer<T>

    /**
     * Returns the [XodusBinding] for the given [Types].
     *
     * @param type The [Types] to lookup a [XodusBinding] for.
     * @return [XodusBinding]
     */
    @Suppress("UNCHECKED_CAST")
    fun <T : Value> xodus(type: Types<T>, nullable: Boolean): XodusBinding<T> = if (nullable) {
       when(type) {
            Types.Boolean -> BooleanValueXodusBinding.Nullable
                Types.Date -> DateValueXodusBinding.Nullable
                Types.Byte -> ByteValueXodusBinding.Nullable
                Types.Complex32 -> Complex32ValueXodusBinding.Nullable
                Types.Complex64 -> Complex64ValueXodusBinding.Nullable
                Types.Double -> DoubleValueXodusBinding.Nullable
                Types.Float -> FloatValueXodusBinding.Nullable
                Types.Int -> IntValueXodusBinding.Nullable
                Types.Long -> LongValueXodusBinding.Nullable
                Types.Short -> ShortValueXodusBinding.Nullable
                Types.String -> StringValueXodusBinding.Nullable
                is Types.BooleanVector -> BooleanVectorValueXodusBinding.Nullable(type.logicalSize)
                is Types.Complex32Vector -> Complex32VectorValueXodusBinding.Nullable(type.logicalSize)
                is Types.Complex64Vector -> Complex64VectorValueXodusBinding.Nullable(type.logicalSize)
                is Types.DoubleVector -> DoubleVectorValueXodusBinding.Nullable(type.logicalSize)
                is Types.FloatVector -> FloatVectorValueXodusBinding.Nullable(type.logicalSize)
                is Types.IntVector -> IntVectorValueXodusBinding.Nullable(type.logicalSize)
                is Types.LongVector -> LongVectorValueXodusBinding.Nullable(type.logicalSize)
                is Types.ByteString -> ByteStringValueXodusBinding.Nullable
       }
    } else {
        when(type) {
            Types.Boolean -> BooleanValueXodusBinding.NonNullable
            Types.Date -> DateValueXodusBinding.NonNullable
            Types.Byte -> ByteValueXodusBinding.NonNullable
            Types.Complex32 -> Complex32ValueXodusBinding.NonNullable
            Types.Complex64 -> Complex64ValueXodusBinding.NonNullable
            Types.Double -> DoubleValueXodusBinding.NonNullable
            Types.Float -> FloatValueXodusBinding.NonNullable
            Types.Int -> IntValueXodusBinding.NonNullable
            Types.Long -> LongValueXodusBinding.NonNullable
            Types.Short -> ShortValueXodusBinding.NonNullable
            Types.String -> StringValueXodusBinding.NonNullable
            is Types.BooleanVector -> BooleanVectorValueXodusBinding.NonNullable(type.logicalSize)
            is Types.Complex32Vector -> Complex32VectorValueXodusBinding.NonNullable(type.logicalSize)
            is Types.Complex64Vector -> Complex64VectorValueXodusBinding.NonNullable(type.logicalSize)
            is Types.DoubleVector -> DoubleVectorValueXodusBinding.NonNullable(type.logicalSize)
            is Types.FloatVector -> FloatVectorValueXodusBinding.NonNullable(type.logicalSize)
            is Types.IntVector -> IntVectorValueXodusBinding.NonNullable(type.logicalSize)
            is Types.LongVector -> LongVectorValueXodusBinding.NonNullable(type.logicalSize)
            is Types.ByteString -> ByteStringValueXodusBinding.NonNullable
        }
    } as XodusBinding<T>
}