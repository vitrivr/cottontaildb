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

    /**
     * Constructor for the collector to get from the sample to the population
     */
    constructor(factor: Float, metrics: DateValueMetrics): this(
        numberOfNullEntries = (metrics.numberOfNullEntries * factor).toLong(),
        numberOfNonNullEntries = (metrics.numberOfNonNullEntries * factor).toLong(),
        numberOfDistinctEntries = (metrics.numberOfDistinctEntries * factor).toLong(),
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
     * Resets this [DateValueMetrics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = DateValue(Long.MAX_VALUE)
        this.max = DateValue(Long.MIN_VALUE)
    }
}