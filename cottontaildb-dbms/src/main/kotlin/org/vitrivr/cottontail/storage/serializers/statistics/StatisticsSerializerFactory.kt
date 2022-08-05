package org.vitrivr.cottontail.storage.serializers.statistics

import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.values.*
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding

/**
 * A [StatisticsSerializerFactory] as used by Cottontail DB to create serializer implementations for its different storage engines.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
object StatisticsSerializerFactory {
    /**
     * Returns the [XodusBinding] for the given [Types].
     *
     * @param type The [Types] to lookup a [XodusBinding] for.
     * @return [XodusBinding]
     */
    @Suppress("UNCHECKED_CAST")
    fun <T : Value> xodus(type: Types<T>): XodusBinding<ValueStatistics<T>> = when(type) {
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
        is Types.BooleanVector -> BooleanVectorValueStatistics.Binding(type.logicalSize)
        is Types.Complex32Vector -> Complex32VectorValueStatistics.Binding(type.logicalSize)
        is Types.Complex64Vector -> Complex64VectorValueStatistics.Binding(type.logicalSize)
        is Types.DoubleVector -> DoubleVectorValueStatistics.Binding(type.logicalSize)
        is Types.FloatVector -> FloatVectorValueStatistics.Binding(type.logicalSize)
        is Types.IntVector -> IntVectorValueStatistics.Binding(type.logicalSize)
        is Types.LongVector -> LongVectorValueStatistics.Binding(type.logicalSize)
    } as XodusBinding<ValueStatistics<T>>
}