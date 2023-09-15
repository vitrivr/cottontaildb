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
    fun <T : Value> metrics(type: Types<T>): MetricsXodusBinding<ValueStatistics<T>> = when(type) {
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
        Types.Uuid -> UuidValueStatistics.Binding
        Types.ByteString -> ByteStringValueStatistics.Binding
        is Types.BooleanVector -> BooleanVectorValueStatistics.Binding(type.logicalSize)
        is Types.Complex32Vector -> Complex32VectorValueStatistics.Binding(type.logicalSize)
        is Types.Complex64Vector -> Complex64VectorValueStatistics.Binding(type.logicalSize)
        is Types.DoubleVector -> DoubleVectorValueStatistics.Binding(type.logicalSize)
        is Types.FloatVector -> FloatVectorValueStatistics.Binding(type.logicalSize)
        is Types.IntVector -> IntVectorValueStatistics.Binding(type.logicalSize)
        is Types.LongVector -> LongVectorValueStatistics.Binding(type.logicalSize)
        is Types.ShortVector -> ShortVectorValueStatistics(type.logicalSize)
    } as MetricsXodusBinding<ValueStatistics<T>>

    /**
     * Returns the [ValueSerializer] for the given [Types].
     *
     * @param type The [Types] to lookup a [ValueSerializer] for.
     * @return [ValueSerializer]
     */
    @Suppress("UNCHECKED_CAST")
    fun <T : Value> value(type: Types<T>): ValueSerializer<T> = when(type) {
        Types.Boolean -> BooleanValueValueSerializer
        Types.Date -> DateValueValueSerializer
        Types.Byte -> ByteValueValueSerializer
        Types.Complex32 -> Complex32ValueValueSerializer
        Types.Complex64 -> Complex64ValueValueSerializer
        Types.Double -> DoubleValueValueSerializer
        Types.Float -> FloatValueValueSerializer
        Types.Int -> IntValueValueSerializer
        Types.Long -> LongValueValueSerializer
        Types.Short -> ShortValueValueSerializer
        Types.String -> StringValueValueSerializer
        Types.Uuid -> UuidValueSerializer
        is Types.BooleanVector -> BooleanVectorValueValueSerializer(type.logicalSize)
        is Types.Complex32Vector -> Complex32VectorValueValueSerializer(type.logicalSize)
        is Types.Complex64Vector -> Complex64VectorValueValueSerializer(type.logicalSize)
        is Types.DoubleVector -> DoubleVectorValueValueSerializer(type.logicalSize)
        is Types.FloatVector -> FloatVectorValueValueSerializer(type.logicalSize)
        is Types.IntVector -> IntVectorValueValueSerializer(type.logicalSize)
        is Types.LongVector -> LongVectorValueValueSerializer(type.logicalSize)
        is Types.ByteString -> ByteStringValueValueSerializer
        is Types.ShortVector -> ShortVectorValueValueSerializer(type.logicalSize)
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