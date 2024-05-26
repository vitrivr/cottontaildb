package org.vitrivr.cottontail.storage.serializers.statistics

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.dbms.statistics.values.IntValueStatistics
import java.io.ByteArrayInputStream

/**
 * A [StatisticsBinding] for [IntValueStatistics].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data object IntValueStatisticsBinding: StatisticsBinding<IntValueStatistics> {
    override fun read(stream: ByteArrayInputStream)= IntValueStatistics(
            LongBinding.readCompressed(stream),
            LongBinding.readCompressed(stream),
            LongBinding.readCompressed(stream),
            IntValue(IntegerBinding.BINDING.readObject(stream)),
            IntValue(IntegerBinding.BINDING.readObject(stream)),
            DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
            DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
            DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
            DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
            DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
        )

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