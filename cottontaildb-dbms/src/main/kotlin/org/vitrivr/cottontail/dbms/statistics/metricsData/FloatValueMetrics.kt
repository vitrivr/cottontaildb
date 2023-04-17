package org.vitrivr.cottontail.dbms.statistics.metricsData

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.bindings.SignedFloatBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.FloatValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueMetrics] implementation for [FloatValue]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
data class FloatValueMetrics (
    override var numberOfNullEntries: Long = 0L,
    override var numberOfNonNullEntries: Long = 0L,
    override var numberOfDistinctEntries: Long = 0L,
    override var min: FloatValue = FloatValue.MAX_VALUE,
    override var max: FloatValue = FloatValue.MIN_VALUE,
    override var sum: DoubleValue = DoubleValue.ZERO
) : RealValueMetrics<FloatValue>(Types.Float) {

    /**
     * Constructor for the collector to get from the sample to the population
     */
    constructor(factor: Float, metrics: FloatValueMetrics): this(
        numberOfNullEntries = (metrics.numberOfNullEntries * factor).toLong(),
        numberOfNonNullEntries = (metrics.numberOfNonNullEntries * factor).toLong(),
        numberOfDistinctEntries = (metrics.numberOfDistinctEntries * factor).toLong(),
        min = metrics.min,
        max = metrics.max,
        sum = DoubleValue(metrics.sum.value * factor)
    )

    /**
     * Xodus serializer for [FloatValueMetrics]
     */
    object Binding: MetricsXodusBinding<FloatValueMetrics> {
        override fun read(stream: ByteArrayInputStream): FloatValueMetrics {
            val numberOfNullEntries = LongBinding.readCompressed(stream)
            val numberOfNonNullEntries = LongBinding.readCompressed(stream)
            val numberOfDistinctEntries = LongBinding.readCompressed(stream)
            val min = FloatValue(SignedFloatBinding.BINDING.readObject(stream))
            val max = FloatValue(SignedFloatBinding.BINDING.readObject(stream))
            val sum = DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
            return FloatValueMetrics(numberOfNullEntries, numberOfNonNullEntries, numberOfDistinctEntries, min, max, sum)
        }

        override fun write(output: LightOutputStream, statistics: FloatValueMetrics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
            SignedFloatBinding.BINDING.writeObject(output, statistics.min.value)
            SignedFloatBinding.BINDING.writeObject(output, statistics.max.value)
            SignedDoubleBinding.BINDING.writeObject(output, statistics.sum.value)
        }
    }

    /**
     * Resets this [FloatValueMetrics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = FloatValue.MAX_VALUE
        this.max = FloatValue.MIN_VALUE
        this.sum = DoubleValue.ZERO
    }
}