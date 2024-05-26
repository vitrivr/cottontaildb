package org.vitrivr.cottontail.storage.serializers.statistics

import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics
import org.vitrivr.cottontail.serialization.buffer.ValueSerializer
import java.io.ByteArrayInputStream

/**
 * A serializer for Xodus based [ValueStatistics] serialization and deserialization.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed interface StatisticsBinding<T: ValueStatistics<*>> {

    companion object {
        /**
         * Returns the [ValueSerializer] for the given [Types].
         *
         * @param type The [Types] to look up a [StatisticsBinding] for.
         * @return [StatisticsBinding]
         */
        @Suppress("UNCHECKED_CAST")
        fun <T : Value> metrics(type: Types<T>) = when(type) {
            Types.Boolean -> BooleanValueStatisticsBinding
            Types.Date -> DateValueStatisticsBinding
            Types.Byte -> ByteValueStatisticsBinding
            Types.Complex32 -> Complex32ValueStatisticsBinding
            Types.Complex64 -> Complex64ValueStatisticsBinding
            Types.Double -> DoubleValueStatisticsBinding
            Types.Float -> FloatValueStatisticsBinding
            Types.Int -> IntValueStatisticsBinding
            Types.Long -> LongValueStatisticsBinding
            Types.Short -> ShortValueStatisticsBinding
            Types.String -> StringValueStatisticsBinding
            Types.Uuid -> UuidValueStatisticsBinding
            Types.ByteString -> ByteStringValueStatisticsBinding
            is Types.BooleanVector -> BooleanVectorValueStatisticsBinding(type.logicalSize)
            is Types.Complex32Vector -> Complex32VectorValueStatisticsBinding(type.logicalSize)
            is Types.Complex64Vector -> Complex64VectorValueStatisticsBinding(type.logicalSize)
            is Types.DoubleVector -> DoubleVectorValueStatisticsBinding(type.logicalSize)
            is Types.HalfVector -> HalfVectorValueStatisticsBinding(type.logicalSize)
            is Types.FloatVector -> FloatVectorValueStatisticsBinding(type.logicalSize)
            is Types.IntVector -> IntVectorValueStatisticsBinding(type.logicalSize)
            is Types.LongVector -> LongVectorValueStatisticsBinding(type.logicalSize)
            is Types.ShortVector -> ShortVectorValueStatisticsBinding(type.logicalSize)
        } as StatisticsBinding<ValueStatistics<T>>
    }

    /**
     * Reads a [ValueStatistics] from the given [ByteArrayInputStream].
     *
     * @param stream [ByteArrayInputStream] to read from
     * @return [ValueStatistics]
     */
    fun read(stream: ByteArrayInputStream): T

    /**
     * Writes a [ValueStatistics] to the given [LightOutputStream].
     *
     * @param output The [LightOutputStream] to write to.
     * @param statistics The [ValueStatistics] to write.
     */
    fun write(output: LightOutputStream, statistics: T)
}