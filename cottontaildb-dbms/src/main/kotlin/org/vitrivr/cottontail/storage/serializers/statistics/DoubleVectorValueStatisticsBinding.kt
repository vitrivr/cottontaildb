package org.vitrivr.cottontail.storage.serializers.statistics

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.statistics.values.DoubleVectorValueStatistics
import java.io.ByteArrayInputStream

/**
 * A [StatisticsBinding] for [DoubleVectorValueStatistics].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DoubleVectorValueStatisticsBinding(val logicalSize: Int): StatisticsBinding<DoubleVectorValueStatistics> {
    override fun read(stream: ByteArrayInputStream): DoubleVectorValueStatistics {
        val numberOfNullEntries = LongBinding.readCompressed(stream)
        val numberOfNonNullEntries = LongBinding.readCompressed(stream)
        val numberOfDistinctEntries = LongBinding.readCompressed(stream)
        val min = DoubleArray(this.logicalSize)
        val max = DoubleArray(this.logicalSize)
        val sum = DoubleArray(this.logicalSize)
        for (i in 0 until this.logicalSize) {
            min[i] = SignedDoubleBinding.BINDING.readObject(stream)
            max[i] = SignedDoubleBinding.BINDING.readObject(stream)
            sum[i] = SignedDoubleBinding.BINDING.readObject(stream)
        }
        return DoubleVectorValueStatistics(this.logicalSize, numberOfNullEntries, numberOfNonNullEntries, numberOfDistinctEntries)
    }

    override fun write(output: LightOutputStream, statistics: DoubleVectorValueStatistics) {
        LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
        for (i in 0 until statistics.type.logicalSize) {
            SignedDoubleBinding.BINDING.writeObject(output, statistics.min.data[i])
            SignedDoubleBinding.BINDING.writeObject(output, statistics.max.data[i])
            SignedDoubleBinding.BINDING.writeObject(output, statistics.sum.data[i])
        }
    }
}