package org.vitrivr.cottontail.dbms.statistics.values

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.BooleanValue
import org.vitrivr.cottontail.storage.serializers.statistics.MetricsXodusBinding
import java.io.ByteArrayInputStream
import kotlin.math.min

/**
 * A [ValueStatistics] implementation for [BooleanValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
data class BooleanValueStatistics(
    override var numberOfNullEntries: Long = 0L,
    override var numberOfNonNullEntries: Long = 0L,
    override var numberOfDistinctEntries: Long = 0L,
    var numberOfTrueEntries: Long = 0L,
    var numberOfFalseEntries: Long = 0L
): AbstractScalarStatistics<BooleanValue>(Types.Boolean) {

    /**
     * Constructor for the collector to get from the sample to the population
     */
    constructor(factor: Float, metrics: BooleanValueStatistics): this(
        numberOfNullEntries = (metrics.numberOfNullEntries * factor).toLong(),
        numberOfNonNullEntries = (metrics.numberOfNonNullEntries * factor).toLong(),
        numberOfDistinctEntries = min(if (metrics.numberOfDistinctEntries.toDouble() / metrics.numberOfEntries.toDouble() >= metrics.distinctEntriesScalingThreshold) (metrics.numberOfDistinctEntries * factor).toLong() else metrics.numberOfDistinctEntries, 2), // Depending on the ratio between distinct entries and number of entries, we either scale the distinct entries (large ratio) or keep them as they are (small ratio). Also don't allow values above 2.
        numberOfTrueEntries = (metrics.numberOfTrueEntries * factor).toLong(),
        numberOfFalseEntries = (metrics.numberOfFalseEntries * factor).toLong()
    )

    companion object {
        const val TRUE_ENTRIES_KEY = "true"
        const val FALSE_ENTRIES_KEY = "false"
    }

    /**
     * Xodus serializer for [BooleanValueStatistics]
     */
    object Binding: MetricsXodusBinding<BooleanValueStatistics> {
        override fun read(stream: ByteArrayInputStream): BooleanValueStatistics {
            val numberOfNullEntries = LongBinding.readCompressed(stream)
            val numberOfNonNullEntries = LongBinding.readCompressed(stream)
            val numberOfDistinctEntries = LongBinding.readCompressed(stream)
            val numberOfTrueEntries = LongBinding.readCompressed(stream)
            val numberOfFalseEntries = LongBinding.readCompressed(stream)
            return BooleanValueStatistics(numberOfNullEntries, numberOfNonNullEntries, numberOfDistinctEntries, numberOfTrueEntries, numberOfFalseEntries)
        }

        override fun write(output: LightOutputStream, statistics: BooleanValueStatistics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
            LongBinding.writeCompressed(output, statistics.numberOfTrueEntries)
            LongBinding.writeCompressed(output, statistics.numberOfFalseEntries)
        }
    }


    /**
     * Creates a descriptive map of this [BooleanValueStatistics].
     *
     * @return Descriptive map of this [BooleanValueStatistics]
     */
    override fun about(): Map<String, String> = super.about() + mapOf(
        TRUE_ENTRIES_KEY to this.numberOfTrueEntries.toString(),
        FALSE_ENTRIES_KEY to this.numberOfFalseEntries.toString(),
    )
}