package org.vitrivr.cottontail.storage.serializers.statistics

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.statistics.values.BooleanVectorValueStatistics
import java.io.ByteArrayInputStream


/**
 * A [StatisticsBinding] for [BooleanVectorValueStatistics].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class BooleanVectorValueStatisticsBinding(val logicalSize: Int): StatisticsBinding<BooleanVectorValueStatistics> {
    override fun read(stream: ByteArrayInputStream) =BooleanVectorValueStatistics(
        this.logicalSize,
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream),
        LongArray(this.logicalSize) { LongBinding.readCompressed(stream) }
    )

    override fun write(output: LightOutputStream, statistics: BooleanVectorValueStatistics) {
        LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
        for (i in 0 until statistics.type.logicalSize) {
            LongBinding.writeCompressed(output, statistics.numberOfTrueEntries[i])
        }
    }
}