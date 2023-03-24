package org.vitrivr.cottontail.dbms.statistics.metricsData

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.ByteStringValue
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import java.io.ByteArrayInputStream

class ByteStringValueMetrics : AbstractValueMetrics<ByteStringValue>(Types.ByteString) {

    object Binding: MetricsXodusBinding<ByteStringValueMetrics> {
        override fun read(stream: ByteArrayInputStream): ByteStringValueMetrics {
            val stat = ByteStringValueMetrics()
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.minWidth = IntegerBinding.readCompressed(stream)
            stat.maxWidth = IntegerBinding.readCompressed(stream)
            return stat
        }

        override fun write(output: LightOutputStream, statistics: ByteStringValueMetrics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            IntegerBinding.writeCompressed(output, statistics.minWidth)
            IntegerBinding.writeCompressed(output, statistics.maxWidth)
        }
    }


    /** Shortest [StringValue] seen by this [ByteStringValueMetrics] */
    override var minWidth: Int = Int.MAX_VALUE
        private set

    /** Longest [StringValue] seen by this [ByteStringValueMetrics]. */
    override var maxWidth: Int = Int.MIN_VALUE
        private set

    override fun reset() {
        super.reset()
        this.minWidth = Int.MAX_VALUE
        this.maxWidth = Int.MIN_VALUE
    }

}