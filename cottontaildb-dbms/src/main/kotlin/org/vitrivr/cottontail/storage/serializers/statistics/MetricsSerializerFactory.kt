package org.vitrivr.cottontail.storage.serializers.statistics

import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.statData.*
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding

/**
 * A [MetricsSerializerFactory] as used by Cottontail DB to create serializer implementations for its different storage engines.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
object MetricsSerializerFactory {
    /**
     * Returns the [XodusBinding] for the given [Types].
     *
     * @param type The [Types] to look up a [MetricsXodusBinding] for.
     * @return [MetricsXodusBinding]
     */
    @Suppress("UNCHECKED_CAST")
    fun <T : Value> xodus(type: Types<T>): MetricsXodusBinding<DataMetrics<T>> = when(type) {
        Types.Boolean -> BooleanValueMetrics.Binding
        Types.Date -> DateValueMetrics.Binding
        Types.Byte -> ByteValueMetrics.Binding
        Types.Complex32 -> Complex32ValueMetrics.Binding
        Types.Complex64 -> Complex64ValueMetrics.Binding
        Types.Double -> DoubleValueMetrics.Binding
        Types.Float -> FloatValueMetrics.Binding
        Types.Int -> IntValueMetrics.Binding
        Types.Long -> LongValueMetrics.Binding
        Types.Short -> ShortValueMetrics.Binding
        Types.String -> StringValueMetrics.Binding
        Types.ByteString -> ByteStringValueMetrics.Binding
        is Types.BooleanVector -> BooleanVectorValueMetrics.Binding(type.logicalSize)
        is Types.Complex32Vector -> Complex32VectorValueMetrics.Binding(type.logicalSize)
        is Types.Complex64Vector -> Complex64VectorValueMetrics.Binding(type.logicalSize)
        is Types.DoubleVector -> DoubleVectorValueMetrics.Binding(type.logicalSize)
        is Types.FloatVector -> FloatVectorValueMetrics.Binding(type.logicalSize)
        is Types.IntVector -> IntVectorValueMetrics.Binding(type.logicalSize)
        is Types.LongVector -> LongVectorValueMetrics.Binding(type.logicalSize)
    } as MetricsXodusBinding<DataMetrics<T>>
}