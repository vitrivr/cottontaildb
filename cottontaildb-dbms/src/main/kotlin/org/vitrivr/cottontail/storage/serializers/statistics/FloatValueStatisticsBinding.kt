package org.vitrivr.cottontail.storage.serializers.statistics

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.bindings.SignedFloatBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.FloatValue
import org.vitrivr.cottontail.dbms.statistics.values.FloatValueStatistics
import java.io.ByteArrayInputStream

/**
 * A [StatisticsBinding] for [FloatValueStatistics].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data object FloatValueStatisticsBinding: StatisticsBinding<FloatValueStatistics> {
    override fun read(stream: ByteArrayInputStream) = FloatValueStatistics(
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream),
        FloatValue(SignedFloatBinding.BINDING.readObject(stream)),
        FloatValue(SignedFloatBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
    )

    override fun write(output: LightOutputStream, statistics: FloatValueStatistics) {
        LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
        SignedFloatBinding.BINDING.writeObject(output, statistics.min.value)
        SignedFloatBinding.BINDING.writeObject(output, statistics.max.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.sum.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.mean.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.variance.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.skewness.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.kurtosis.value)
    }
}
