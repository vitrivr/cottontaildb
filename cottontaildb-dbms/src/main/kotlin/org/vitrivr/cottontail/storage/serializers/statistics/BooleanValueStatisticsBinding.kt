package org.vitrivr.cottontail.storage.serializers.statistics

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.statistics.values.BooleanValueStatistics
import java.io.ByteArrayInputStream

/**
 * A [StatisticsBinding] for [BooleanValueStatistics].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data object BooleanValueStatisticsBinding: StatisticsBinding<BooleanValueStatistics> {
    override fun read(stream: ByteArrayInputStream) = BooleanValueStatistics(
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream)
    )
    override fun write(output: LightOutputStream, statistics: BooleanValueStatistics) {
        LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
        LongBinding.writeCompressed(output, statistics.numberOfTrueEntries)
        LongBinding.writeCompressed(output, statistics.numberOfFalseEntries)
    }
}