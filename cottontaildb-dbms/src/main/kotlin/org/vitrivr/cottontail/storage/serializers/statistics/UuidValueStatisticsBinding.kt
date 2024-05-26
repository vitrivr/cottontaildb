package org.vitrivr.cottontail.storage.serializers.statistics

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.statistics.values.UuidValueStatistics
import java.io.ByteArrayInputStream

/**
 * A [StatisticsBinding] for [UuidValueStatistics].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data object UuidValueStatisticsBinding: StatisticsBinding<UuidValueStatistics> {
    override fun read(stream: ByteArrayInputStream): UuidValueStatistics {
        val numberOfNullEntries = LongBinding.readCompressed(stream)
        val numberOfNonNullEntries = LongBinding.readCompressed(stream)
        val numberOfDistinctEntries = LongBinding.readCompressed(stream)
        return UuidValueStatistics(numberOfNullEntries, numberOfNonNullEntries, numberOfDistinctEntries)
    }

    override fun write(output: LightOutputStream, statistics: UuidValueStatistics) {
        LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
    }
}