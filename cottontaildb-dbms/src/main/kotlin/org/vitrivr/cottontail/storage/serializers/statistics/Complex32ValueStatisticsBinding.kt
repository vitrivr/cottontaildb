package org.vitrivr.cottontail.storage.serializers.statistics

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.statistics.values.Complex32ValueStatistics
import java.io.ByteArrayInputStream

/**
 * A [StatisticsBinding] for [Complex32ValueStatistics].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data object Complex32ValueStatisticsBinding: StatisticsBinding<Complex32ValueStatistics> {
    override fun read(stream: ByteArrayInputStream) = Complex32ValueStatistics(
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream)
    )

    override fun write(output: LightOutputStream, statistics: Complex32ValueStatistics) {
        LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
    }
}