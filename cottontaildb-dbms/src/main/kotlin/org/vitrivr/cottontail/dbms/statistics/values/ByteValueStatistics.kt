package org.vitrivr.cottontail.dbms.statistics.values

import jetbrains.exodus.bindings.ByteBinding
import jetbrains.exodus.bindings.DoubleBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ByteValue
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.storage.serializers.statistics.MetricsXodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueStatistics] implementation for [ByteValue]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
data class ByteValueStatistics(
    override var numberOfNullEntries: Long = 0L,
    override var numberOfNonNullEntries: Long = 0L,
    override var numberOfDistinctEntries: Long = 0L,
    override var min: ByteValue = ByteValue.MAX_VALUE,
    override var max: ByteValue = ByteValue.MIN_VALUE,
    override var sum: DoubleValue = DoubleValue.ZERO,
    override var mean: DoubleValue = DoubleValue.ZERO,
    override var variance: DoubleValue = DoubleValue.ZERO,
    override var skewness: DoubleValue = DoubleValue.ZERO,
    override var kurtosis: DoubleValue = DoubleValue.ZERO
) : RealValueStatistics<ByteValue>(Types.Byte) {

    /**
     * Constructor for the collector to get from the sample to the population
     */
    constructor(factor: Float, metrics: ByteValueStatistics): this(
        numberOfNullEntries = (metrics.numberOfNullEntries * factor).toLong(),
        numberOfNonNullEntries = (metrics.numberOfNonNullEntries * factor).toLong(),
        numberOfDistinctEntries = if (metrics.numberOfDistinctEntries.toDouble() / metrics.numberOfEntries.toDouble() >= metrics.distinctEntriesScalingThreshold) (metrics.numberOfDistinctEntries * factor).toLong() else metrics.numberOfDistinctEntries, // Depending on the ratio between distinct entries and number of entries, we either scale the distinct entries (large ratio) or keep them as they are (small ratio).
        min = metrics.min,
        max = metrics.max,
        sum = DoubleValue(metrics.sum.value * factor),
        mean = metrics.mean,
        variance = metrics.variance,
        skewness = metrics.skewness,
        kurtosis = metrics.kurtosis
    )

    /**
     * Xodus serializer for [ByteValueStatistics]
     */
    object Binding: MetricsXodusBinding<ByteValueStatistics> {
        override fun read(stream: ByteArrayInputStream): ByteValueStatistics {
            val numberOfNullEntries = LongBinding.readCompressed(stream)
            val numberOfNonNullEntries = LongBinding.readCompressed(stream)
            val numberOfDistinctEntries = LongBinding.readCompressed(stream)
            val min = ByteValue(ByteBinding.BINDING.readObject(stream))
            val max = ByteValue(ByteBinding.BINDING.readObject(stream))
            val sum = DoubleValue(DoubleBinding.BINDING.readObject(stream))
            val mean = DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
            val variance = DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
            val skewness = DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
            val kurtosis = DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
            return ByteValueStatistics(
                numberOfNullEntries,
                numberOfNonNullEntries,
                numberOfDistinctEntries,
                min,
                max,
                sum,
                mean,
                variance,
                skewness,
                kurtosis)
        }

        override fun write(output: LightOutputStream, statistics: ByteValueStatistics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
            ByteBinding.BINDING.writeObject(output, statistics.min.value)
            ByteBinding.BINDING.writeObject(output, statistics.max.value)
            DoubleBinding.BINDING.writeObject(output, statistics.sum.value)
            SignedDoubleBinding.BINDING.writeObject(output, statistics.mean.value)
            SignedDoubleBinding.BINDING.writeObject(output, statistics.variance.value)
            SignedDoubleBinding.BINDING.writeObject(output, statistics.skewness.value)
            SignedDoubleBinding.BINDING.writeObject(output, statistics.kurtosis.value)
        }
    }

}