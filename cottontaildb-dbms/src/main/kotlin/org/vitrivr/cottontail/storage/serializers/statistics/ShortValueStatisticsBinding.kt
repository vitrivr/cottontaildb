package org.vitrivr.cottontail.storage.serializers.statistics

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.ShortBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.ShortValue
import org.vitrivr.cottontail.dbms.statistics.values.ShortValueStatistics
import java.io.ByteArrayInputStream

/**
 * A [StatisticsBinding] for [ShortValueStatistics].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data object ShortValueStatisticsBinding: StatisticsBinding<ShortValueStatistics> {
    override fun read(stream: ByteArrayInputStream) = ShortValueStatistics(
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream),
        ShortValue(ShortBinding.BINDING.readObject(stream)),
        ShortValue(ShortBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream)),
        DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
    )

    override fun write(output: LightOutputStream, statistics: ShortValueStatistics) {
        LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
        ShortBinding.BINDING.writeObject(output, statistics.min.value)
        ShortBinding.BINDING.writeObject(output, statistics.max.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.sum.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.mean.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.variance.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.skewness.value)
        SignedDoubleBinding.BINDING.writeObject(output, statistics.kurtosis.value)
    }
}