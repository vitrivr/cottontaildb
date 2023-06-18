package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.statistics.values.*
import org.vitrivr.cottontail.storage.serializers.statistics.MetricsXodusBinding
import org.vitrivr.cottontail.storage.serializers.tablets.*
import org.vitrivr.cottontail.storage.serializers.values.*

/**
 * A [SerializerFactory] as used by Cottontail DB to create serializer implementations for its different storage engines.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object SerializerFactory {

    /**
     * Returns the [ValueSerializer] for the given [Types].
     *
     * @param type The [Types] to look up a [MetricsXodusBinding] for.
     * @return [MetricsXodusBinding]
     */
    @Suppress("UNCHECKED_CAST")
    fun <T : Value> xodus(type: Types<T>): MetricsXodusBinding<ValueStatistics<T>> = when(type) {
        Types.Boolean -> BooleanValueStatistics.Binding
        Types.Date -> DateValueStatistics.Binding
        Types.Byte -> ByteValueStatistics.Binding
        Types.Complex32 -> Complex32ValueStatistics.Binding
        Types.Complex64 -> Complex64ValueStatistics.Binding
        Types.Double -> DoubleValueStatistics.Binding
        Types.Float -> FloatValueStatistics.Binding
        Types.Int -> IntValueStatistics.Binding
        Types.Long -> LongValueStatistics.Binding
        Types.Short -> ShortValueStatistics.Binding
        Types.String -> StringValueStatistics.Binding
        Types.ByteString -> ByteStringValueStatistics.Binding
        is Types.BooleanVector -> BooleanVectorValueStatistics.Binding(type.logicalSize)
        is Types.Complex32Vector -> Complex32VectorValueStatistics.Binding(type.logicalSize)
        is Types.Complex64Vector -> Complex64VectorValueStatistics.Binding(type.logicalSize)
        is Types.DoubleVector -> DoubleVectorValueStatistics.Binding(type.logicalSize)
        is Types.FloatVector -> FloatVectorValueStatistics.Binding(type.logicalSize)
        is Types.IntVector -> IntVectorValueStatistics.Binding(type.logicalSize)
        is Types.LongVector -> LongVectorValueStatistics.Binding(type.logicalSize)
    } as MetricsXodusBinding<ValueStatistics<T>>

    /**
     * Returns the [ValueSerializer] for the given [Types].
     *
     * @param type The [Types] to lookup a [ValueSerializer] for.
     * @return [ValueSerializer]
     */
    @Suppress("UNCHECKED_CAST")
    fun <T : Value> value(type: Types<T>, nullable: Boolean): ValueSerializer<T> = if (nullable) {
       when(type) {
            Types.Boolean -> BooleanValueValueSerializer.Nullable
                Types.Date -> DateValueValueSerializer.Nullable
                Types.Byte -> ByteValueValueSerializer.Nullable
                Types.Complex32 -> Complex32ValueValueSerializer.Nullable
                Types.Complex64 -> Complex64ValueValueSerializer.Nullable
                Types.Double -> DoubleValueValueSerializer.Nullable
                Types.Float -> FloatValueValueSerializer.Nullable
                Types.Int -> IntValueValueSerializer.Nullable
                Types.Long -> LongValueValueSerializer.Nullable
                Types.Short -> ShortValueValueSerializer.Nullable
                Types.String -> StringValueValueSerializer.Nullable
                is Types.BooleanVector -> BooleanVectorValueValueSerializer.Nullable(type.logicalSize)
                is Types.Complex32Vector -> Complex32VectorValueValueSerializer.Nullable(type.logicalSize)
                is Types.Complex64Vector -> Complex64VectorValueValueSerializer.Nullable(type.logicalSize)
                is Types.DoubleVector -> DoubleVectorValueValueSerializer.Nullable(type.logicalSize)
                is Types.FloatVector -> FloatVectorValueValueSerializer.Nullable(type.logicalSize)
                is Types.IntVector -> IntVectorValueValueSerializer.Nullable(type.logicalSize)
                is Types.LongVector -> LongVectorValueValueSerializer.Nullable(type.logicalSize)
                is Types.ByteString -> ByteStringValueValueSerializer.Nullable
       }
    } else {
        when(type) {
            Types.Boolean -> BooleanValueValueSerializer.NonNullable
            Types.Date -> DateValueValueSerializer.NonNullable
            Types.Byte -> ByteValueValueSerializer.NonNullable
            Types.Complex32 -> Complex32ValueValueSerializer.NonNullable
            Types.Complex64 -> Complex64ValueValueSerializer.NonNullable
            Types.Double -> DoubleValueValueSerializer.NonNullable
            Types.Float -> FloatValueValueSerializer.NonNullable
            Types.Int -> IntValueValueSerializer.NonNullable
            Types.Long -> LongValueValueSerializer.NonNullable
            Types.Short -> ShortValueValueSerializer.NonNullable
            Types.String -> StringValueValueSerializer.NonNullable
            is Types.BooleanVector -> BooleanVectorValueValueSerializer.NonNullable(type.logicalSize)
            is Types.Complex32Vector -> Complex32VectorValueValueSerializer.NonNullable(type.logicalSize)
            is Types.Complex64Vector -> Complex64VectorValueValueSerializer.NonNullable(type.logicalSize)
            is Types.DoubleVector -> DoubleVectorValueValueSerializer.NonNullable(type.logicalSize)
            is Types.FloatVector -> FloatVectorValueValueSerializer.NonNullable(type.logicalSize)
            is Types.IntVector -> IntVectorValueValueSerializer.NonNullable(type.logicalSize)
            is Types.LongVector -> LongVectorValueValueSerializer.NonNullable(type.logicalSize)
            is Types.ByteString -> ByteStringValueValueSerializer.NonNullable
        }
    } as ValueSerializer<T>

    /**
     * Returns the [ValueSerializer] for the given [Types].
     *
     * @param type The [Types] to lookup a [ValueSerializer] for.
     * @return [ValueSerializer]
     */
    @Suppress("UNCHECKED_CAST")
    fun <T : Value> tablet(type: Types<T>, size: Int, compression: Compression = Compression.LZ4): TabletSerializer<T> = when(compression) {
        Compression.NONE -> NoneTabletSerializer(type, size)
        Compression.LZ4 -> LZ4TabletSerializer(type, size)
        Compression.SNAPPY -> SnappyTabletSerializer(type, size)
    }
}