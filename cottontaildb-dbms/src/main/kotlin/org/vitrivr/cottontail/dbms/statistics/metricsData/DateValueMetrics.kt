package org.vitrivr.cottontail.dbms.statistics.metricsData

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.DateValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueMetrics] implementation for [DateValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class DateValueMetrics : AbstractValueMetrics<DateValue>(Types.Date) {

    /**
     * Xodus serializer for [DateValueMetrics]
     */
    object Binding: MetricsXodusBinding<DateValueMetrics> {
        override fun read(stream: ByteArrayInputStream): DateValueMetrics {
            val stat = DateValueMetrics()
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.min = DateValue(LongBinding.readCompressed(stream))
            stat.max = DateValue(LongBinding.readCompressed(stream))
            return stat
        }

        override fun write(output: LightOutputStream, statistics: DateValueMetrics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.writeCompressed(output, statistics.min.value)
            LongBinding.writeCompressed(output, statistics.max.value)
        }
    }

    /** Minimum value seen by this [DateValueMetrics]. */
    var min: DateValue = DateValue(Long.MAX_VALUE)
        private set

    /** Minimum value seen by this [DateValueMetrics]. */
    var max: DateValue = DateValue(Long.MIN_VALUE)
            private set

    /**
     * Resets this [DateValueMetrics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = DateValue(Long.MAX_VALUE)
        this.max = DateValue(Long.MIN_VALUE)
    }
}