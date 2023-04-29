package org.vitrivr.cottontail.dbms.statistics.metricsData

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.BooleanValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import java.io.ByteArrayInputStream
import kotlin.math.min

/**
 * A [ValueMetrics] implementation for [BooleanValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
data class BooleanValueMetrics(
    override var numberOfNullEntries: Long = 0L,
    override var numberOfNonNullEntries: Long = 0L,
    override var numberOfDistinctEntries: Long = 0L,
    var numberOfTrueEntries: Long = 0L,
    var numberOfFalseEntries: Long = 0L
): AbstractScalarMetrics<BooleanValue>(Types.Boolean) {

    /**
     * Constructor for the collector to get from the sample to the population
     */
    constructor(factor: Float, metrics: BooleanValueMetrics): this(
        numberOfNullEntries = (metrics.numberOfNullEntries * factor).toLong(),
        numberOfNonNullEntries = (metrics.numberOfNonNullEntries * factor).toLong(),
        numberOfDistinctEntries = min((metrics.numberOfDistinctEntries * factor).toLong(), 2), // since in the boolean setting can only be 2 distinct entries
        numberOfTrueEntries = (metrics.numberOfTrueEntries * factor).toLong(),
        numberOfFalseEntries = (metrics.numberOfFalseEntries * factor).toLong()
    )

    companion object {
        const val TRUE_ENTRIES_KEY = "true"
        const val FALSE_ENTRIES_KEY = "false"
    }

    /**
     * Xodus serializer for [BooleanValueMetrics]
     */
    object Binding: MetricsXodusBinding<BooleanValueMetrics> {
        override fun read(stream: ByteArrayInputStream): BooleanValueMetrics {
            val numberOfNullEntries = LongBinding.readCompressed(stream)
            val numberOfNonNullEntries = LongBinding.readCompressed(stream)
            val numberOfDistinctEntries = LongBinding.readCompressed(stream)
            val numberOfTrueEntries = LongBinding.readCompressed(stream)
            val numberOfFalseEntries = LongBinding.readCompressed(stream)
            return BooleanValueMetrics(numberOfNullEntries, numberOfNonNullEntries, numberOfDistinctEntries, numberOfTrueEntries, numberOfFalseEntries)
        }

        override fun write(output: LightOutputStream, statistics: BooleanValueMetrics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
            LongBinding.writeCompressed(output, statistics.numberOfTrueEntries)
            LongBinding.writeCompressed(output, statistics.numberOfFalseEntries)
        }
    }


    /**
     * Creates a descriptive map of this [BooleanValueMetrics].
     *
     * @return Descriptive map of this [BooleanValueMetrics]
     */
    override fun about(): Map<String, String> = super.about() + mapOf(
        TRUE_ENTRIES_KEY to this.numberOfTrueEntries.toString(),
        FALSE_ENTRIES_KEY to this.numberOfFalseEntries.toString(),
    )
}