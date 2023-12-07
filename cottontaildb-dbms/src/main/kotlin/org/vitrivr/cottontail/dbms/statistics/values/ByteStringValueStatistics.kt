package org.vitrivr.cottontail.dbms.statistics.values

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ByteStringValue
import org.vitrivr.cottontail.storage.serializers.statistics.MetricsXodusBinding
import java.io.ByteArrayInputStream

data class ByteStringValueStatistics(
    override var numberOfNullEntries: Long = 0L,
    override var numberOfNonNullEntries: Long = 0L,
    override var numberOfDistinctEntries: Long = 0L,
    override var minWidth: Int = Int.MAX_VALUE,
    override var maxWidth: Int = Int.MIN_VALUE
) : AbstractScalarStatistics<ByteStringValue>(Types.ByteString) {

    /**
     * Constructor for the collector to get from the sample to the population
     */
    constructor(factor: Float, metrics: ByteStringValueStatistics): this(
        numberOfNullEntries = (metrics.numberOfNullEntries * factor).toLong(),
        numberOfNonNullEntries = (metrics.numberOfNonNullEntries * factor).toLong(),
        numberOfDistinctEntries = if (metrics.numberOfDistinctEntries.toDouble() / metrics.numberOfEntries.toDouble() >= metrics.distinctEntriesScalingThreshold) (metrics.numberOfDistinctEntries * factor).toLong() else metrics.numberOfDistinctEntries, // Depending on the ratio between distinct entries and number of entries, we either scale the distinct entries (large ratio) or keep them as they are (small ratio).
        minWidth = metrics.minWidth,
        maxWidth = metrics.maxWidth,
    )

    object Binding: MetricsXodusBinding<ByteStringValueStatistics> {
        override fun read(stream: ByteArrayInputStream): ByteStringValueStatistics {
            val numberOfNullEntries = LongBinding.readCompressed(stream)
            val numberOfNonNullEntries = LongBinding.readCompressed(stream)
            val numberOfDistinctEntries = LongBinding.readCompressed(stream)
            val minWidth = IntegerBinding.readCompressed(stream)
            val maxWidth = IntegerBinding.readCompressed(stream)
            return ByteStringValueStatistics(numberOfNullEntries, numberOfNonNullEntries, numberOfDistinctEntries, minWidth, maxWidth)
        }

        override fun write(output: LightOutputStream, statistics: ByteStringValueStatistics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
            IntegerBinding.writeCompressed(output, statistics.minWidth)
            IntegerBinding.writeCompressed(output, statistics.maxWidth)
        }
    }

    /**
     * Creates a descriptive map of this [ValueStatistics].
     *
     * @return Descriptive map of this [ValueStatistics]
     */
    override fun about(): Map<String, String> {
        return super.about() + mapOf(
            MIN_WIDTH_KEY to this.minWidth.toString(),
            MAX_WIDTH_KEY to this.maxWidth.toString()
        )
    }

}