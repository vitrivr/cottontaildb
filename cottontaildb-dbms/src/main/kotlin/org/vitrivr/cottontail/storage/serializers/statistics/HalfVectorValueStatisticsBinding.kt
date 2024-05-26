package org.vitrivr.cottontail.storage.serializers.statistics

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.bindings.SignedFloatBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.statistics.values.HalfVectorValueStatistics
import java.io.ByteArrayInputStream

/**
 * A [StatisticsBinding] for [HalfVectorValueStatistics].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class HalfVectorValueStatisticsBinding(val logicalSize: Int): StatisticsBinding<HalfVectorValueStatistics> {
    override fun read(stream: ByteArrayInputStream): HalfVectorValueStatistics {
        val numberOfNullEntries = LongBinding.readCompressed(stream)
        val numberOfNonNullEntries = LongBinding.readCompressed(stream)
        val numberOfDistinctEntries = LongBinding.readCompressed(stream)
        val min = FloatArray(this.logicalSize)
        val max = FloatArray(this.logicalSize)
        val sum = DoubleArray(this.logicalSize)
        for (i in 0 until this.logicalSize) {
            min[i] = SignedFloatBinding.BINDING.readObject(stream)
            max[i] = SignedFloatBinding.BINDING.readObject(stream)
            sum[i] = SignedDoubleBinding.BINDING.readObject(stream)
        }
        return HalfVectorValueStatistics(this.logicalSize, numberOfNullEntries, numberOfNonNullEntries, numberOfDistinctEntries)
    }

    override fun write(output: LightOutputStream, statistics: HalfVectorValueStatistics) {
        LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
        for (i in 0 until statistics.type.logicalSize) {
            SignedFloatBinding.BINDING.writeObject(output, statistics.min.data[i])
            SignedFloatBinding.BINDING.writeObject(output, statistics.max.data[i])
            SignedDoubleBinding.BINDING.writeObject(output, statistics.sum.data[i])
        }
    }
}