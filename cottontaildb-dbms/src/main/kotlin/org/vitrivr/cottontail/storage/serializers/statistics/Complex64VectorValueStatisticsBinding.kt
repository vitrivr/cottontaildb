package org.vitrivr.cottontail.storage.serializers.statistics

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.statistics.values.Complex64VectorValueStatistics
import java.io.ByteArrayInputStream

/**
 * A [StatisticsBinding] for [Complex64VectorValueStatistics].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex64VectorValueStatisticsBinding(val logicalSize: Int): StatisticsBinding<Complex64VectorValueStatistics> {
    override fun read(stream: ByteArrayInputStream) =  Complex64VectorValueStatistics(
        this.logicalSize,
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream)
    )

    override fun write(output: LightOutputStream, statistics: Complex64VectorValueStatistics) {
        LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
    }
}