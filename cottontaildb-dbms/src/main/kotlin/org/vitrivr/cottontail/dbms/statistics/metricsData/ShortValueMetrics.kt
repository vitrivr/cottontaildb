package org.vitrivr.cottontail.dbms.statistics.metricsData

import jetbrains.exodus.bindings.DoubleBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.ShortBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.ByteValue
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.ShortValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueMetrics] implementation for [ShortValue]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
data class ShortValueMetrics(
    override var numberOfNullEntries: Long = 0L,
    override var numberOfNonNullEntries: Long = 0L,
    override var numberOfDistinctEntries: Long = 0L,
    override var min: ShortValue = ShortValue.MAX_VALUE,
    override var max: ShortValue = ShortValue.MIN_VALUE,
    override var sum: DoubleValue = DoubleValue.ZERO
) : RealValueMetrics<ShortValue>(Types.Short) {

    /**
     * Constructor for the collector to get from the sample to the population
     */
    constructor(factor: Float, metrics: ShortValueMetrics): this(
        numberOfNullEntries = (metrics.numberOfNullEntries * factor).toLong(),
        numberOfNonNullEntries = (metrics.numberOfNonNullEntries * factor).toLong(),
        numberOfDistinctEntries = (metrics.numberOfDistinctEntries * factor).toLong(),
        min = metrics.min,
        max = metrics.max,
        sum = DoubleValue(metrics.sum.value * factor)
    )

    /**
     * Xodus serializer for [ShortValueMetrics]
     */
    object Binding: MetricsXodusBinding<ShortValueMetrics> {
        override fun read(stream: ByteArrayInputStream): ShortValueMetrics {
            val numberOfNullEntries = LongBinding.readCompressed(stream)
            val numberOfNonNullEntries = LongBinding.readCompressed(stream)
            val numberOfDistinctEntries = LongBinding.readCompressed(stream)
            val min = ShortValue(ShortBinding.BINDING.readObject(stream))
            val max = ShortValue(ShortBinding.BINDING.readObject(stream))
            val sum = DoubleValue(DoubleBinding.BINDING.readObject(stream))
            return ShortValueMetrics(numberOfNullEntries, numberOfNonNullEntries, numberOfDistinctEntries, min, max)
        }

        override fun write(output: LightOutputStream, statistics: ShortValueMetrics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
            ShortBinding.BINDING.writeObject(output, statistics.min.value)
            ShortBinding.BINDING.writeObject(output, statistics.max.value)
            DoubleBinding.BINDING.writeObject(output, statistics.sum.value)
        }
    }

    /**
     * Resets this [ShortValueMetrics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = ShortValue.MAX_VALUE
        this.max = ShortValue.MIN_VALUE
        this.sum = DoubleValue.ZERO
    }

}