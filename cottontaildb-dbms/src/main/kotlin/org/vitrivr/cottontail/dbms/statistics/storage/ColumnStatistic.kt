package org.vitrivr.cottontail.dbms.statistics.storage

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.util.ByteArraySizedInputStream
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.column.Column
import org.vitrivr.cottontail.dbms.statistics.values.*
import org.vitrivr.cottontail.storage.serializers.SerializerFactory
import org.vitrivr.cottontail.storage.serializers.statistics.MetricsXodusBinding

/**
 * A [ColumnStatistic] in the Cottontail DB [Catalogue]. Used to store metrics about [Column]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.1.0
 */
data class ColumnStatistic(val type: Types<*>, val statistics: ValueStatistics<*>) {

    companion object {
        /**
         * De-serializes a [ColumnStatistic] from the given [ByteIterable].
         *
         * @param entry The [ByteIterable] to read entry from.
         */
        fun entryToObject(entry: ByteIterable): ColumnStatistic{
            val stream = ByteArraySizedInputStream(entry.bytesUnsafe, 0, entry.length)
            val type = Types.forOrdinal(IntegerBinding.readCompressed(stream), IntegerBinding.readCompressed(stream))
            val serializer = SerializerFactory.metrics(type)
            return ColumnStatistic(type, serializer.read(stream))
        }
        /**
         * Serializes a [ColumnStatistic] to a [ArrayByteIterable].
         *
         * @param `object` The [ColumnStatistic] to serialize.
         * @return [ArrayByteIterable]
         */
        @Suppress("UNCHECKED_CAST")
        fun objectToEntry(`object`: ColumnStatistic): ArrayByteIterable {
            val output = LightOutputStream()
            IntegerBinding.writeCompressed(output, `object`.type.ordinal)
            IntegerBinding.writeCompressed(output, `object`.type.logicalSize)
            val serializer = SerializerFactory.metrics(`object`.type) as MetricsXodusBinding<ValueStatistics<*>>
            serializer.write(output, `object`.statistics)
            return output.asArrayByteIterable()
        }
    }

    /**
     * Creates a [ColumnStatistic] from the provided [ColumnDef].
     *
     * @param def The [ColumnDef] to convert.
     */
    constructor(def: ColumnDef<*>) : this(def.type, when (def.type) {
            Types.Boolean -> BooleanValueStatistics()
            Types.Byte -> ByteValueStatistics()
            Types.Short -> ShortValueStatistics()
            Types.Date -> DateValueStatistics()
            Types.Double -> DoubleValueStatistics()
            Types.Float -> FloatValueStatistics()
            Types.Int -> IntValueStatistics()
            Types.Long -> LongValueStatistics()
            Types.String -> StringValueStatistics()
            Types.ByteString -> ByteStringValueStatistics()
            Types.Complex32 -> Complex32ValueStatistics()
            Types.Complex64 -> Complex64ValueStatistics()
            Types.Uuid -> UuidValueStatistics()
            is Types.BooleanVector -> BooleanVectorValueStatistics(def.type.logicalSize)
            is Types.DoubleVector -> DoubleVectorValueStatistics(def.type.logicalSize)
            is Types.FloatVector -> FloatVectorValueStatistics(def.type.logicalSize)
            is Types.IntVector -> IntVectorValueStatistics(def.type.logicalSize)
            is Types.LongVector -> LongVectorValueStatistics(def.type.logicalSize)
            is Types.Complex32Vector -> Complex32VectorValueStatistics(def.type.logicalSize)
            is Types.Complex64Vector -> Complex64VectorValueStatistics(def.type.logicalSize)
            is Types.ShortVector -> ShortVectorValueStatistics(def.type.logicalSize)
        }
    )
}