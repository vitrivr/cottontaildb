package org.vitrivr.cottontail.dbms.statistics.metricsData

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.ByteValue
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueMetrics] implementation for [IntValue]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
data class IntValueMetrics (
    override var numberOfNullEntries: Long = 0L,
    override var numberOfNonNullEntries: Long = 0L,
    override var numberOfDistinctEntries: Long = 0L,
    override var min: IntValue = IntValue.MAX_VALUE,
    override var max: IntValue = IntValue.MIN_VALUE,
    override var sum: DoubleValue = DoubleValue.ZERO
) : RealValueMetrics<IntValue>(Types.Int) {

    /**
     * Constructor for the collector to get from the sample to the population
     */
    constructor(factor: Float, metrics: IntValueMetrics): this(
        numberOfNullEntries = (metrics.numberOfNullEntries * factor).toLong(),
        numberOfNonNullEntries = (metrics.numberOfNonNullEntries * factor).toLong(),
        numberOfDistinctEntries = (metrics.numberOfDistinctEntries * factor).toLong(),
        min = metrics.min,
        max = metrics.max,
        sum = DoubleValue(metrics.sum.value * factor)
    )

    /**
     * Xodus serializer for [IntValueMetrics]
     */
    object Binding : MetricsXodusBinding<IntValueMetrics> {
        override fun read(stream: ByteArrayInputStream): IntValueMetrics {
            val numberOfNullEntries = LongBinding.readCompressed(stream)
            val numberOfNonNullEntries = LongBinding.readCompressed(stream)
            val numberOfDistinctEntries = LongBinding.readCompressed(stream)
            val min = IntValue(IntegerBinding.BINDING.readObject(stream))
            val max = IntValue(IntegerBinding.BINDING.readObject(stream))
            val sum = DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
            return IntValueMetrics(numberOfNullEntries, numberOfNonNullEntries, numberOfDistinctEntries, min, max, sum)
        }

        override fun write(output: LightOutputStream, statistics: IntValueMetrics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
            IntegerBinding.BINDING.writeObject(output, statistics.min.value)
            IntegerBinding.BINDING.writeObject(output, statistics.max.value)
            SignedDoubleBinding.BINDING.writeObject(output, statistics.sum.value)
        }
    }

    /**
     * Resets this [IntValueMetrics] and sets all its values to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = IntValue.MAX_VALUE
        this.max = IntValue.MIN_VALUE
    }
}
