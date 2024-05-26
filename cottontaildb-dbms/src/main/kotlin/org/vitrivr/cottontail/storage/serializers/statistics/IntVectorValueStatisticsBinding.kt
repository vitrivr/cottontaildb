package org.vitrivr.cottontail.storage.serializers.statistics

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.statistics.values.IntVectorValueStatistics
import java.io.ByteArrayInputStream

/**
 * A [StatisticsBinding] for [IntVectorValueStatistics].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class IntVectorValueStatisticsBinding(val logicalSize: Int): StatisticsBinding<IntVectorValueStatistics> {
    override fun read(stream: ByteArrayInputStream): IntVectorValueStatistics {
        val numberOfNullEntries = LongBinding.readCompressed(stream)
        val numberOfNonNullEntries = LongBinding.readCompressed(stream)
        val numberOfDistinctEntries = LongBinding.readCompressed(stream)
        val min = IntArray(this.logicalSize)
        val max = IntArray(this.logicalSize)
        val sum = DoubleArray(this.logicalSize)
        for (i in 0 until this.logicalSize) {
            min[i] = IntegerBinding.BINDING.readObject(stream)
            max[i] = IntegerBinding.BINDING.readObject(stream)
            sum[i] = SignedDoubleBinding.BINDING.readObject(stream)
        }
        return IntVectorValueStatistics(this.logicalSize, numberOfNullEntries, numberOfNonNullEntries, numberOfDistinctEntries)
    }

    override fun write(output: LightOutputStream, statistics: IntVectorValueStatistics) {
        LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
        for (i in 0 until statistics.type.logicalSize) {
            IntegerBinding.BINDING.writeObject(output, statistics.min.data[i])
            IntegerBinding.BINDING.writeObject(output, statistics.max.data[i])
            SignedDoubleBinding.BINDING.writeObject(output, statistics.sum.data[i])
        }
    }
}