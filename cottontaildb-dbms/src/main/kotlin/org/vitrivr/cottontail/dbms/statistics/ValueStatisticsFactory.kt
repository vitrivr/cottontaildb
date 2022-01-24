package org.vitrivr.cottontail.dbms.statistics

import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.columns.*

/**
 * A factory class for [ValueStat]
 */
object ValueStatisticsFactory {
    /**
     * Generates and returns a [ValueStatistics] object for the given [Types].
     *
     * @param type [Types] to create [ValueStatistics] for.
     * @return [ValueStatistics]
     */
    @Suppress("UNCHECKED_CAST")
    fun <T: Value> create(type: Types<T>): ValueStatistics<T> = when(type) {
        Types.Boolean -> BooleanValueStatistics()
        Types.Date -> DateValueStatistics()
        Types.Byte -> ByteValueStatistics()
        Types.Double -> DoubleValueStatistics()
        Types.Float -> FloatValueStatistics()
        Types.Int -> IntValueStatistics()
        Types.Long -> LongValueStatistics()
        Types.Short -> ShortValueStatistics()
        Types.String -> StringValueStatistics()
        is Types.BooleanVector -> BooleanVectorValueStatistics(type)
        is Types.DoubleVector -> DoubleVectorValueStatistics(type)
        is Types.FloatVector -> FloatVectorValueStatistics(type)
        is Types.IntVector -> IntVectorValueStatistics(type)
        is Types.LongVector -> LongVectorValueStatistics(type)
        else -> ValueStatistics(type)
    } as ValueStatistics<T>
}