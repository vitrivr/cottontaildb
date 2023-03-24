package org.vitrivr.cottontail.dbms.statistics.metricsData

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import java.io.ByteArrayInputStream

/**
 * A specialized [ValueMetrics] implementation for [StringValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class StringValueMetrics : AbstractValueMetrics<StringValue>(Types.String) {

    /**
     * Xodus serializer for [StringValueMetrics]
     */
    object Binding: MetricsXodusBinding<StringValueMetrics> {
        override fun read(stream: ByteArrayInputStream): StringValueMetrics {
            val stat = StringValueMetrics()
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.minWidth = IntegerBinding.readCompressed(stream)
            stat.maxWidth = IntegerBinding.readCompressed(stream)
            return stat
        }

        override fun write(output: LightOutputStream, statistics: StringValueMetrics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            IntegerBinding.writeCompressed(output, statistics.minWidth)
            IntegerBinding.writeCompressed(output, statistics.maxWidth)
        }
    }

    /** Shortest [StringValue] seen by this [StringValueMetrics] */
    override var minWidth: Int = Int.MAX_VALUE
        private set

    /** Longest [StringValue] seen by this [StringValueMetrics]. */
    override var maxWidth: Int = Int.MIN_VALUE
        private set

    /**
     * Resets this [StringValueMetrics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.minWidth = Int.MAX_VALUE
        this.maxWidth = Int.MIN_VALUE
    }

}