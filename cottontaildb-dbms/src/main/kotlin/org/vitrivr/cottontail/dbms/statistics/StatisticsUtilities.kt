package org.vitrivr.cottontail.dbms.statistics

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.dbms.statistics.values.*


/**
 * Estimates the size of a [Tuple] based on the [ColumnDef] and the associated [ValueStatistics].
 *
 * @return The estimated size of the [Tuple] in bytes.
 */
fun Map<ColumnDef<*>, ValueStatistics<*>>.estimateTupleSize() = this.map {
    when (val type = it.key.type) {
        Types.String -> it.value.avgWidth * Char.SIZE_BYTES
        Types.ByteString -> it.value.avgWidth
        else -> type.physicalSize
    }
}.sum()

/**
 * Generates and returns the default [ValueStatistics] for this [Types].
 *
 * @return [ValueStatistics] matching this [Types]
 */
fun Types<*>.defaultStatistics() = when(this) {
    Types.Boolean -> BooleanValueStatistics()
    Types.Byte -> ByteValueStatistics()
    Types.Short -> ShortValueStatistics()
    Types.Int -> IntValueStatistics()
    Types.Long -> LongValueStatistics()
    Types.Float -> FloatValueStatistics()
    Types.Double -> DoubleValueStatistics()
    Types.Complex32 -> Complex32ValueStatistics()
    Types.Complex64 -> Complex64ValueStatistics()
    Types.Date -> DateValueStatistics()
    Types.ByteString -> ByteValueStatistics()
    Types.String -> StringValueStatistics()
    is Types.BooleanVector -> BooleanVectorValueStatistics(this.logicalSize)
    is Types.IntVector -> IntVectorValueStatistics(this.logicalSize)
    is Types.LongVector -> LongVectorValueStatistics(this.logicalSize)
    is Types.FloatVector -> FloatVectorValueStatistics(this.logicalSize)
    is Types.DoubleVector -> DoubleVectorValueStatistics(this.logicalSize)
    is Types.Complex32Vector -> Complex32VectorValueStatistics(this.logicalSize)
    is Types.Complex64Vector -> Complex64VectorValueStatistics(this.logicalSize)
}