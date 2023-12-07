package org.vitrivr.cottontail.dbms.statistics.values

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex64Value
import org.vitrivr.cottontail.storage.serializers.statistics.MetricsXodusBinding
import java.io.ByteArrayInputStream


/**
 * A [ValueStatistics] implementation for [Complex64Value]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class Complex64ValueStatistics(
    override var numberOfNullEntries: Long = 0L,
    override var numberOfNonNullEntries: Long = 0L,
    override var numberOfDistinctEntries: Long = 0L,
): AbstractScalarStatistics<Complex64Value>(Types.Complex64) {

    /**
     * Constructor for the collector to get from the sample to the population
     */
    constructor(factor: Float, metrics: Complex64ValueStatistics): this(
        numberOfNullEntries = (metrics.numberOfNullEntries * factor).toLong(),
        numberOfNonNullEntries = (metrics.numberOfNonNullEntries * factor).toLong(),
        numberOfDistinctEntries = if (metrics.numberOfDistinctEntries.toDouble() / metrics.numberOfEntries.toDouble() >= metrics.distinctEntriesScalingThreshold) (metrics.numberOfDistinctEntries * factor).toLong() else metrics.numberOfDistinctEntries, // Depending on the ratio between distinct entries and number of entries, we either scale the distinct entries (large ratio) or keep them as they are (small ratio).
    )

    /**
     * Xodus serializer for [Complex64ValueStatistics]
     */
    object Binding: MetricsXodusBinding<Complex64ValueStatistics> {
        override fun read(stream: ByteArrayInputStream): Complex64ValueStatistics {
            val numberOfNullEntries = LongBinding.readCompressed(stream)
            val numberOfNonNullEntries = LongBinding.readCompressed(stream)
            val numberOfDistinctEntries = LongBinding.readCompressed(stream)
            return Complex64ValueStatistics(numberOfNullEntries, numberOfNonNullEntries, numberOfDistinctEntries)
        }

        override fun write(output: LightOutputStream, statistics: Complex64ValueStatistics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
        }
    }

}