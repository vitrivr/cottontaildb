package org.vitrivr.cottontail.dbms.statistics.metricsData

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.ByteStringValue
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import java.io.ByteArrayInputStream

data class ByteStringValueMetrics(
    override var numberOfNullEntries: Long = 0L,
    override var numberOfNonNullEntries: Long = 0L,
    override var numberOfDistinctEntries: Long = 0L,
    override var minWidth: Int = Int.MAX_VALUE,
    override var maxWidth: Int = Int.MIN_VALUE
) : AbstractScalarMetrics<ByteStringValue>(Types.ByteString) {


    object Binding: MetricsXodusBinding<ByteStringValueMetrics> {
        override fun read(stream: ByteArrayInputStream): ByteStringValueMetrics {
            val numberOfNullEntries = LongBinding.readCompressed(stream)
            val numberOfNonNullEntries = LongBinding.readCompressed(stream)
            val numberOfDistinctEntries = LongBinding.readCompressed(stream)
            val minWidth = IntegerBinding.readCompressed(stream)
            val maxWidth = IntegerBinding.readCompressed(stream)
            return ByteStringValueMetrics(numberOfNullEntries, numberOfNonNullEntries, numberOfDistinctEntries, minWidth, maxWidth)
        }

        override fun write(output: LightOutputStream, statistics: ByteStringValueMetrics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
            IntegerBinding.writeCompressed(output, statistics.minWidth)
            IntegerBinding.writeCompressed(output, statistics.maxWidth)
        }
    }


    override fun reset() {
        super.reset()
        this.minWidth = Int.MAX_VALUE
        this.maxWidth = Int.MIN_VALUE
    }

}