package org.vitrivr.cottontail.dbms.statistics.metricsData

import jetbrains.exodus.bindings.ByteBinding
import jetbrains.exodus.bindings.DoubleBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.ByteValue
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import java.io.ByteArrayInputStream
import kotlin.math.min

/**
 * A [ValueMetrics] implementation for [ByteValue]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
data class ByteValueMetrics(
    override var numberOfNullEntries: Long = 0L,
    override var numberOfNonNullEntries: Long = 0L,
    override var numberOfDistinctEntries: Long = 0L,
    override var min: ByteValue = ByteValue.MAX_VALUE,
    override var max: ByteValue = ByteValue.MIN_VALUE,
    override var sum: DoubleValue = DoubleValue.ZERO
) : RealValueMetrics<ByteValue>(Types.Byte) {

    /**
     * Constructor for the collector to get from the sample to the population
     */
    constructor(factor: Float, metrics: ByteValueMetrics): this(
        numberOfNullEntries = (metrics.numberOfNullEntries * factor).toLong(),
        numberOfNonNullEntries = (metrics.numberOfNonNullEntries * factor).toLong(),
        numberOfDistinctEntries = (metrics.numberOfDistinctEntries * factor).toLong(),
        min = metrics.min,
        max = metrics.max,
        sum = DoubleValue(metrics.sum.value * factor)
    )

    /**
     * Xodus serializer for [ByteValueMetrics]
     */
    object Binding: MetricsXodusBinding<ByteValueMetrics> {
        override fun read(stream: ByteArrayInputStream): ByteValueMetrics {
            val numberOfNullEntries = LongBinding.readCompressed(stream)
            val numberOfNonNullEntries = LongBinding.readCompressed(stream)
            val numberOfDistinctEntries = LongBinding.readCompressed(stream)
            val min = ByteValue(ByteBinding.BINDING.readObject(stream))
            val max = ByteValue(ByteBinding.BINDING.readObject(stream))
            val sum = DoubleValue(DoubleBinding.BINDING.readObject(stream))
            return ByteValueMetrics(numberOfNullEntries, numberOfNonNullEntries, numberOfDistinctEntries, min, max, sum)
        }

        override fun write(output: LightOutputStream, statistics: ByteValueMetrics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
            ByteBinding.BINDING.writeObject(output, statistics.min.value)
            ByteBinding.BINDING.writeObject(output, statistics.max.value)
            DoubleBinding.BINDING.writeObject(output, statistics.sum.value)
        }
    }

    /**
     * Resets this [ByteValueMetrics] and sets all its values to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = ByteValue.MAX_VALUE
        this.max = ByteValue.MIN_VALUE
        this.sum = DoubleValue.ZERO
    }

}