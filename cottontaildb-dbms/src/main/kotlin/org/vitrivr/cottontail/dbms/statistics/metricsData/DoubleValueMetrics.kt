package org.vitrivr.cottontail.dbms.statistics.metricsData

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.ByteValue
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueMetrics] implementation for [DoubleValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
data class DoubleValueMetrics (
    override var numberOfNullEntries: Long = 0L,
    override var numberOfNonNullEntries: Long = 0L,
    override var numberOfDistinctEntries: Long = 0L,
    override var min: DoubleValue = DoubleValue.MAX_VALUE,
    override var max: DoubleValue = DoubleValue.MIN_VALUE,
    override var sum: DoubleValue = DoubleValue.ZERO
) : RealValueMetrics<DoubleValue>(Types.Double) {

    /**
     * Constructor for the collector to get from the sample to the population
     */
    constructor(factor: Float, metrics: DoubleValueMetrics): this(
        numberOfNullEntries = (metrics.numberOfNullEntries * factor).toLong(),
        numberOfNonNullEntries = (metrics.numberOfNonNullEntries * factor).toLong(),
        numberOfDistinctEntries = (metrics.numberOfDistinctEntries * factor).toLong(),
        min = metrics.min,
        max = metrics.max,
        sum = DoubleValue(metrics.sum.value * factor)
    )

    /**
     * Xodus serializer for [DoubleValueMetrics]
     */
    object Binding: MetricsXodusBinding<DoubleValueMetrics> {
        override fun read(stream: ByteArrayInputStream): DoubleValueMetrics {
            val numberOfNullEntries = LongBinding.readCompressed(stream)
            val numberOfNonNullEntries = LongBinding.readCompressed(stream)
            val numberOfDistinctEntries = LongBinding.readCompressed(stream)
            val min = DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
            val max = DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
            val sum = DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
            return DoubleValueMetrics(numberOfNullEntries, numberOfNonNullEntries, numberOfDistinctEntries, min, max, sum)
        }

        override fun write(output: LightOutputStream, statistics: DoubleValueMetrics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
            SignedDoubleBinding.BINDING.writeObject(output, statistics.min.value)
            SignedDoubleBinding.BINDING.writeObject(output, statistics.max.value)
            SignedDoubleBinding.BINDING.writeObject(output, statistics.sum.value)
        }
    }

    /**
     * Resets this [DoubleValueMetrics] and sets all its values to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = DoubleValue.MAX_VALUE
        this.max = DoubleValue.MIN_VALUE
        this.sum = DoubleValue.ZERO
    }
}