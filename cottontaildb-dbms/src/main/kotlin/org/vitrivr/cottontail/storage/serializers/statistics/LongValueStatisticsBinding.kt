package org.vitrivr.cottontail.storage.serializers.statistics

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.dbms.statistics.values.LongValueStatistics
import java.io.ByteArrayInputStream

/**
 * A [StatisticsBinding] for [LongValueStatistics].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data object LongValueStatisticsBinding : StatisticsBinding<LongValueStatistics> {
    override fun read(stream: ByteArrayInputStream) = LongValueStatistics(
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream),
        LongValue(LongBinding.BINDING.readObject(stream)),
        LongValue(LongBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
    )

    override fun write(output: LightOutputStream, statistics: LongValueStatistics) {
        LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
        LongBinding.BINDING.writeObject(output, statistics.min.value)
        LongBinding.BINDING.writeObject(output, statistics.max.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.sum.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.mean.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.variance.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.skewness.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.kurtosis.value)
    }
}