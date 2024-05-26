package org.vitrivr.cottontail.storage.serializers.statistics

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.dbms.statistics.values.DoubleValueStatistics
import org.vitrivr.cottontail.dbms.statistics.values.FloatValueStatistics
import java.io.ByteArrayInputStream

/**
 * A [StatisticsBinding] for [FloatValueStatistics].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data object DoubleValueStatisticsBinding: StatisticsBinding<DoubleValueStatistics> {
    override fun read(stream: ByteArrayInputStream) = DoubleValueStatistics(
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
    )

    override fun write(output: LightOutputStream, statistics: DoubleValueStatistics) {
        LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.min.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.max.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.sum.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.mean.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.variance.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.skewness.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.kurtosis.value)
    }
}