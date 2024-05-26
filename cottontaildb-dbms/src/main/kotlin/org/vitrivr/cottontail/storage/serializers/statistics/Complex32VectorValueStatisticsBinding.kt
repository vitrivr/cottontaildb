package org.vitrivr.cottontail.storage.serializers.statistics

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.statistics.values.Complex32VectorValueStatistics
import java.io.ByteArrayInputStream

/**
 * A [StatisticsBinding] for [Complex32VectorValueStatistics].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex32VectorValueStatisticsBinding(val logicalSize: Int): StatisticsBinding<Complex32VectorValueStatistics> {
    override fun read(stream: ByteArrayInputStream) =  Complex32VectorValueStatistics(
        this.logicalSize,
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream)
    )

    override fun write(output: LightOutputStream, statistics: Complex32VectorValueStatistics) {
        LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
    }
}