package org.vitrivr.cottontail.dbms.statistics.metricsData

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.BooleanVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueMetrics] implementation for [BooleanVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
data class BooleanVectorValueMetrics(
    val logicalSize: Int,
    override var numberOfNullEntries: Long = 0L,
    override var numberOfNonNullEntries: Long = 0L,
    override var numberOfDistinctEntries: Long = 0L,
    var numberOfTrueEntries: LongArray = LongArray(logicalSize),
    ) : AbstractVectorMetrics<BooleanVectorValue>(Types.BooleanVector(logicalSize)) {

    /**
     * Constructor for the collector to get from the sample to the population
     */
    constructor(factor: Float, metrics: BooleanVectorValueMetrics): this(
        logicalSize = metrics.logicalSize,
        numberOfNullEntries = (metrics.numberOfNullEntries * factor).toLong(),
        numberOfNonNullEntries = (metrics.numberOfNonNullEntries * factor).toLong(),
        numberOfDistinctEntries = (metrics.numberOfDistinctEntries * factor).toLong(),
        numberOfTrueEntries = metrics.numberOfTrueEntries.map { element -> (element * factor).toLong() }.toLongArray()
    )

    /**
     * Xodus serializer for [BooleanVectorValueMetrics]
     */
    class Binding(val logicalSize: Int): MetricsXodusBinding<BooleanVectorValueMetrics> {
        override fun read(stream: ByteArrayInputStream): BooleanVectorValueMetrics {
            val stat = BooleanVectorValueMetrics(this@Binding.logicalSize)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfDistinctEntries = LongBinding.readCompressed(stream)
            for (i in 0 until this@Binding.logicalSize) {
                stat.numberOfTrueEntries[i] = LongBinding.readCompressed(stream)
                //stat.numberOfFalseEntries[i] = LongBinding.readCompressed(stream) // false entries don't have to be read since they're computed
            }
            return stat
        }

        override fun write(output: LightOutputStream, statistics: BooleanVectorValueMetrics) {
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

    /**
     * Resets this [BooleanVectorValueMetrics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        for (i in 0 until this.type.logicalSize) {
            this.numberOfTrueEntries[i] = 0L
        }
    }
}