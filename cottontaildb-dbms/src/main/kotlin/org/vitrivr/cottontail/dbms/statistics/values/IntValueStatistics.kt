package org.vitrivr.cottontail.dbms.statistics.values

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.storage.serializers.statistics.MetricsXodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueStatistics] implementation for [IntValue]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
data class IntValueStatistics (
    override var numberOfNullEntries: Long = 0L,
    override var numberOfNonNullEntries: Long = 0L,
    override var numberOfDistinctEntries: Long = 0L,
    override var min: IntValue = IntValue.MAX_VALUE,
    override var max: IntValue = IntValue.MIN_VALUE,
    override var sum: DoubleValue = DoubleValue.ZERO,
    override var mean: DoubleValue = DoubleValue.ZERO,
    override var variance: DoubleValue = DoubleValue.ZERO,
    override var skewness: DoubleValue = DoubleValue.ZERO,
    override var kurtosis: DoubleValue = DoubleValue.ZERO

) : RealValueStatistics<IntValue>(Types.Int) {

    /**
     * Constructor for the collector to get from the sample to the population
     */
    constructor(factor: Float, metrics: IntValueStatistics): this(
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
     * Xodus serializer for [IntValueStatistics]
     */
    object Binding : MetricsXodusBinding<IntValueStatistics> {
        override fun read(stream: ByteArrayInputStream): IntValueStatistics {
            val numberOfNullEntries = LongBinding.readCompressed(stream)
            val numberOfNonNullEntries = LongBinding.readCompressed(stream)
            val numberOfDistinctEntries = LongBinding.readCompressed(stream)
            val min = IntValue(IntegerBinding.BINDING.readObject(stream))
            val max = IntValue(IntegerBinding.BINDING.readObject(stream))
            val sum = DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
            val mean = DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
            val variance = DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
            val skewness = DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
            val kurtosis = DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
            return IntValueStatistics(
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

        override fun write(output: LightOutputStream, statistics: IntValueStatistics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
            IntegerBinding.BINDING.writeObject(output, statistics.min.value)
            IntegerBinding.BINDING.writeObject(output, statistics.max.value)
            SignedDoubleBinding.BINDING.writeObject(output, statistics.sum.value)
            SignedDoubleBinding.BINDING.writeObject(output, statistics.mean.value)
            SignedDoubleBinding.BINDING.writeObject(output, statistics.variance.value)
            SignedDoubleBinding.BINDING.writeObject(output, statistics.skewness.value)
            SignedDoubleBinding.BINDING.writeObject(output, statistics.kurtosis.value)
        }
    }

}
