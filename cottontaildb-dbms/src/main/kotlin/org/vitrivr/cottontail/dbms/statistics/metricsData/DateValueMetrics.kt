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
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.2.0
 */
data class DateValueMetrics (
    override var numberOfNullEntries: Long = 0L,
    override var numberOfNonNullEntries: Long = 0L,
    override var numberOfDistinctEntries: Long = 0L,
    var min: DateValue = DateValue(Long.MAX_VALUE),
    var max: DateValue = DateValue(Long.MIN_VALUE),
) : AbstractScalarMetrics<DateValue>(Types.Date) {

    companion object {
        const val MIN_KEY = "min"
        const val MAX_KEY = "max"
    }

    /**
     * Constructor for the collector to get from the sample to the population
     */
    constructor(factor: Float, metrics: DateValueMetrics): this(
        numberOfNullEntries = (metrics.numberOfNullEntries * factor).toLong(),
        numberOfNonNullEntries = (metrics.numberOfNonNullEntries * factor).toLong(),
        numberOfDistinctEntries = if (metrics.numberOfDistinctEntries.toDouble() / metrics.numberOfEntries.toDouble() >= metrics.distinctEntriesScalingThreshold) (metrics.numberOfDistinctEntries * factor).toLong() else metrics.numberOfDistinctEntries, // Depending on the ratio between distinct entries and number of entries, we either scale the distinct entries (large ratio) or keep them as they are (small ratio).
        min = metrics.min,
        max = metrics.max,
    )

    /**
     * Xodus serializer for [DateValueMetrics]
     */
    object Binding: MetricsXodusBinding<DateValueMetrics> {
        override fun read(stream: ByteArrayInputStream): DateValueMetrics {
            val numberOfNullEntries = LongBinding.readCompressed(stream)
            val numberOfNonNullEntries = LongBinding.readCompressed(stream)
            val numberOfDistinctEntries = LongBinding.readCompressed(stream)
            val min = DateValue(LongBinding.readCompressed(stream))
            val max = DateValue(LongBinding.readCompressed(stream))
            return DateValueMetrics(numberOfNullEntries, numberOfNonNullEntries, numberOfDistinctEntries, min, max)
        }

        override fun write(output: LightOutputStream, statistics: DateValueMetrics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
            LongBinding.writeCompressed(output, statistics.min.value)
            LongBinding.writeCompressed(output, statistics.max.value)
        }
    }

    /**
     * Creates a descriptive map of this [ValueMetrics].
     *
     * @return Descriptive map of this [ValueMetrics]
     */
    override fun about(): Map<String, String> {
        return super.about() + mapOf(
            MIN_KEY to this.min.value.toString(),
            MAX_KEY to this.max.value.toString()
        )
    }
}