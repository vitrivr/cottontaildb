package org.vitrivr.cottontail.storage.serializers.statistics

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.statistics.values.LongVectorValueStatistics
import java.io.ByteArrayInputStream

/**
 * A [StatisticsBinding] for [LongVectorValueStatistics].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LongVectorValueStatisticsBinding(val logicalSize: Int): StatisticsBinding<LongVectorValueStatistics> {
    override fun read(stream: ByteArrayInputStream): LongVectorValueStatistics {
        val numberOfNullEntries = LongBinding.readCompressed(stream)
        val numberOfNonNullEntries = LongBinding.readCompressed(stream)
        val numberOfDistinctEntries = LongBinding.readCompressed(stream)
        val min = LongArray(this.logicalSize)
        val max = LongArray(this.logicalSize)
        val sum = DoubleArray(this.logicalSize)
        for (i in 0 until this.logicalSize) {
            min[i] = LongBinding.BINDING.readObject(stream)
            max[i] = LongBinding.BINDING.readObject(stream)
            sum[i] = SignedDoubleBinding.BINDING.readObject(stream)
        }
        return LongVectorValueStatistics(this.logicalSize, numberOfNullEntries, numberOfNonNullEntries, numberOfDistinctEntries)
    }

    override fun write(output: LightOutputStream, statistics: LongVectorValueStatistics) {
        LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
        for (i in 0 until statistics.type.logicalSize) {
            LongBinding.BINDING.writeObject(output, statistics.min.data[i])
            LongBinding.BINDING.writeObject(output, statistics.max.data[i])
            SignedDoubleBinding.BINDING.writeObject(output, statistics.sum.data[i])
        }
    }
}