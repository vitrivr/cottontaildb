package org.vitrivr.cottontail.dbms.statistics.values

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.BooleanVectorValue
import org.vitrivr.cottontail.storage.serializers.statistics.MetricsXodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueStatistics] implementation for [BooleanVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
data class BooleanVectorValueStatistics(
    val logicalSize: Int,
    override var numberOfNullEntries: Long = 0L,
    override var numberOfNonNullEntries: Long = 0L,
    override var numberOfDistinctEntries: Long = 0L,
    var numberOfTrueEntries: LongArray = LongArray(logicalSize),
    ) : AbstractVectorStatistics<BooleanVectorValue>(Types.BooleanVector(logicalSize)) {

    /**
     * Constructor for the collector to get from the sample to the population
     */
    constructor(factor: Float, metrics: BooleanVectorValueStatistics): this(
        logicalSize = metrics.logicalSize,
        numberOfNullEntries = (metrics.numberOfNullEntries * factor).toLong(),
        numberOfNonNullEntries = (metrics.numberOfNonNullEntries * factor).toLong(),
        numberOfDistinctEntries = if (metrics.numberOfDistinctEntries.toDouble() / metrics.numberOfEntries.toDouble() >= metrics.distinctEntriesScalingThreshold) (metrics.numberOfDistinctEntries * factor).toLong() else metrics.numberOfDistinctEntries, // Depending on the ratio between distinct entries and number of entries, we either scale the distinct entries (large ratio) or keep them as they are (small ratio).
        numberOfTrueEntries = metrics.numberOfTrueEntries.map { element -> (element * factor).toLong() }.toLongArray()
    )

    /**
     * Xodus serializer for [BooleanVectorValueStatistics]
     */
    class Binding(val logicalSize: Int): MetricsXodusBinding<BooleanVectorValueStatistics> {
        override fun read(stream: ByteArrayInputStream): BooleanVectorValueStatistics {
            val stat = BooleanVectorValueStatistics(this@Binding.logicalSize)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfDistinctEntries = LongBinding.readCompressed(stream)
            for (i in 0 until this@Binding.logicalSize) {
                stat.numberOfTrueEntries[i] = LongBinding.readCompressed(stream)
                //stat.numberOfFalseEntries[i] = LongBinding.readCompressed(stream) // false entries don't have to be read since they're computed
            }
            return stat
        }

        override fun write(output: LightOutputStream, statistics: BooleanVectorValueStatistics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
            for (i in 0 until statistics.type.logicalSize) {
                LongBinding.writeCompressed(output, statistics.numberOfTrueEntries[i])
                // LongBinding.writeCompressed(output, statistics.numberOfFalseEntries[i]) // false entries don't have to be written since they're computed
            }
        }
    }

    /** A histogram capturing the number of true entries per component. */
    //var numberOfTrueEntries: LongArray = LongArray(this.type.logicalSize) // initialized via constructor

    /** A histogram capturing the number of false entries per component. */
    val numberOfFalseEntries: LongArray
        get() = LongArray(this.type.logicalSize) {
            this.numberOfNonNullEntries - this.numberOfTrueEntries[it]
        }
}