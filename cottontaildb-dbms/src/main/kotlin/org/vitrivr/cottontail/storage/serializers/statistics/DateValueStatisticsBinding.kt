package org.vitrivr.cottontail.storage.serializers.statistics

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.DateValue
import org.vitrivr.cottontail.dbms.statistics.values.DateValueStatistics
import java.io.ByteArrayInputStream

/**
 * A [StatisticsBinding] for [DateValueStatistics].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data object DateValueStatisticsBinding : StatisticsBinding<DateValueStatistics> {
    override fun read(stream: ByteArrayInputStream) = DateValueStatistics(
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream),
        LongBinding.readCompressed(stream),
        DateValue(LongBinding.readCompressed(stream)), DateValue(LongBinding.readCompressed(stream))
    )
    override fun write(output: LightOutputStream, statistics: DateValueStatistics) {
        LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
        LongBinding.writeCompressed(output, statistics.min.value)
        LongBinding.writeCompressed(output, statistics.max.value)
    }
}