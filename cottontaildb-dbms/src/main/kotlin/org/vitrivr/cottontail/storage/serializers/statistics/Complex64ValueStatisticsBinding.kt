package org.vitrivr.cottontail.storage.serializers.statistics

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.statistics.values.Complex64ValueStatistics
import java.io.ByteArrayInputStream

/**
 * A [StatisticsBinding] for [Complex64ValueStatistics].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data object Complex64ValueStatisticsBinding: StatisticsBinding<Complex64ValueStatistics> {
    override fun read(stream: ByteArrayInputStream) = Complex64ValueStatistics(
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream)
    )

    override fun write(output: LightOutputStream, statistics: Complex64ValueStatistics) {
        LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
    }
}