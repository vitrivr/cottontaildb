package org.vitrivr.cottontail.storage.serializers.statistics

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.statistics.values.ByteStringValueStatistics
import java.io.ByteArrayInputStream

/**
 * A [StatisticsBinding] for [ByteStringValueStatistics].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data object ByteStringValueStatisticsBinding: StatisticsBinding<ByteStringValueStatistics> {
    override fun read(stream: ByteArrayInputStream) = ByteStringValueStatistics(
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream),
        IntegerBinding.readCompressed(stream),
        IntegerBinding.readCompressed(stream)
    )

    override fun write(output: LightOutputStream, statistics: ByteStringValueStatistics) {
        LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
        IntegerBinding.writeCompressed(output, statistics.minWidth)
        IntegerBinding.writeCompressed(output, statistics.maxWidth)
    }
}