package org.vitrivr.cottontail.storage.serializers.statistics

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.statistics.values.StringValueStatistics
import java.io.ByteArrayInputStream

/**
 * A [StatisticsBinding] for [StringValueStatistics].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data object StringValueStatisticsBinding: StatisticsBinding<StringValueStatistics> {
    override fun read(stream: ByteArrayInputStream): StringValueStatistics {
        val numberOfNullEntries = LongBinding.readCompressed(stream)
        val numberOfNonNullEntries = LongBinding.readCompressed(stream)
        val numberOfDistinctEntries = LongBinding.readCompressed(stream)
        val minWidth = IntegerBinding.readCompressed(stream)
        val maxWidth = IntegerBinding.readCompressed(stream)
        return StringValueStatistics(numberOfNullEntries, numberOfNonNullEntries, numberOfDistinctEntries, minWidth, maxWidth)
    }

    override fun write(output: LightOutputStream, statistics: StringValueStatistics) {
        LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
        IntegerBinding.writeCompressed(output, statistics.minWidth)
        IntegerBinding.writeCompressed(output, statistics.maxWidth)
    }
}
