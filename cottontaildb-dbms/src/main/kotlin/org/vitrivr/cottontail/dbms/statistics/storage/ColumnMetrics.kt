package org.vitrivr.cottontail.dbms.statistics.storage

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.column.Column
import org.vitrivr.cottontail.dbms.statistics.*
import org.vitrivr.cottontail.dbms.statistics.collectors.*
import org.vitrivr.cottontail.dbms.statistics.values.*
import org.vitrivr.cottontail.storage.serializers.statistics.MetricsSerializerFactory
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ColumnMetrics] in the Cottontail DB [Catalogue]. Used to store metrics about [Column]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.1.0
 */
data class ColumnMetrics(val name: Name.ColumnName, val type: Types<*>, val statistics: ValueStatistics<*>) {

    /**
     * Creates a [ColumnMetrics] from the provided [ColumnDef].
     *
     * @param def The [ColumnDef] to convert.
     */
    constructor(def: ColumnDef<*>) : this(
        def.name, def.type, when (def.type) {
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
            is Types.BooleanVector -> BooleanVectorValueStatistics(def.type.logicalSize)
            is Types.DoubleVector -> DoubleVectorValueStatistics(def.type.logicalSize)
            is Types.FloatVector -> FloatVectorValueStatistics(def.type.logicalSize)
            is Types.IntVector -> IntVectorValueStatistics(def.type.logicalSize)
            is Types.LongVector -> LongVectorValueStatistics(def.type.logicalSize)
            is Types.Complex32Vector -> Complex32VectorValueStatistics(def.type.logicalSize)
            is Types.Complex64Vector -> Complex64VectorValueStatistics(def.type.logicalSize)
        }
    )

    /**
     * Creates a [Serialized] version of this [ColumnMetrics].
     *
     * @return [Serialized]
     */
    fun toSerialized() = Serialized(this.type, this.statistics)

    /**
     * The [Serialized] version of the [ColumnMetrics]. That entry does not include the [Name.ColumnName].
     */
    data class Serialized(val type: Types<*>, val statistics: ValueStatistics<*>): Comparable<Serialized> {

        /**
         * Converts this [Serialized] to an actual [ColumnMetrics].
         *
         * @param name The [Name.ColumnName] this entry belongs to.
         * @return [ColumnMetrics]
         */
        fun toActual(name: Name.ColumnName) = ColumnMetrics(name, this.type, this.statistics)

        companion object: ComparableBinding() {
            /**
             * De-serializes a [Serialized] from the given [ByteArrayInputStream].
             */
            override fun readObject(stream: ByteArrayInputStream): Serialized {
                val type = Types.forOrdinal(IntegerBinding.readCompressed(stream), IntegerBinding.readCompressed(stream))
                val serializer = MetricsSerializerFactory.xodus(type)
                return Serialized(type, serializer.read(stream))
            }

            /**
             * Serializes a [Serialized] to the given [LightOutputStream].
             */
            @Suppress("UNCHECKED_CAST")
            override fun writeObject(output: LightOutputStream, `object`: Comparable<Serialized>) {
                require(`object` is Serialized) { "$`object` cannot be written as statistics entry." }
                IntegerBinding.writeCompressed(output, `object`.type.ordinal)
                IntegerBinding.writeCompressed(output, `object`.type.logicalSize)
                val serializer = MetricsSerializerFactory.xodus(`object`.type) as MetricsXodusBinding<ValueStatistics<*>>
                serializer.write(output, `object`.statistics)
            }
        }
        override fun compareTo(other: Serialized): Int = this.type.ordinal.compareTo(other.type.ordinal)
    }
}