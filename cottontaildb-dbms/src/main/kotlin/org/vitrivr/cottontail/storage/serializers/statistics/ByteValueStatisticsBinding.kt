package org.vitrivr.cottontail.storage.serializers.statistics

import jetbrains.exodus.bindings.ByteBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.ByteValue
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.dbms.statistics.values.ByteValueStatistics
import java.io.ByteArrayInputStream

/**
 * A [StatisticsBinding] for [ByteValueStatistics].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data object ByteValueStatisticsBinding: StatisticsBinding<ByteValueStatistics> {
    override fun read(stream: ByteArrayInputStream) = ByteValueStatistics(
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream),
        ByteValue(ByteBinding.BINDING.readObject(stream)),
        ByteValue(ByteBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
    )

    override fun write(output: LightOutputStream, statistics: ByteValueStatistics) {
        LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
        ByteBinding.BINDING.writeObject(output, statistics.min.value)
        ByteBinding.BINDING.writeObject(output, statistics.max.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.sum.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.mean.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.variance.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.skewness.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.kurtosis.value)
    }
}