package org.vitrivr.cottontail.dbms.statistics.values

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueStatistics] implementation for [DoubleVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
data class DoubleVectorValueStatistics(
    val logicalSize: Int,
    override var numberOfNullEntries: Long = 0L,
    override var numberOfNonNullEntries: Long = 0L,
    override var numberOfDistinctEntries: Long = 0L,
    override val min: DoubleVectorValue = DoubleVectorValue(DoubleArray(logicalSize) { Double.MAX_VALUE }),
    override val max: DoubleVectorValue = DoubleVectorValue(DoubleArray(logicalSize) { Double.MIN_VALUE }),
    override val sum: DoubleVectorValue = DoubleVectorValue(DoubleArray(logicalSize))
) : RealVectorValueStatistics<DoubleVectorValue>(Types.DoubleVector(logicalSize)) {

    /**
     * Constructor for the collector to get from the sample to the population
     */
    constructor(factor: Float, metrics: DoubleVectorValueStatistics): this(
        logicalSize = metrics.logicalSize,
        numberOfNullEntries = (metrics.numberOfNullEntries * factor).toLong(),
        numberOfNonNullEntries = (metrics.numberOfNonNullEntries * factor).toLong(),
        numberOfDistinctEntries = if (metrics.numberOfDistinctEntries.toDouble() / metrics.numberOfEntries.toDouble() >= metrics.distinctEntriesScalingThreshold) (metrics.numberOfDistinctEntries * factor).toLong() else metrics.numberOfDistinctEntries, // Depending on the ratio between distinct entries and number of entries, we either scale the distinct entries (large ratio) or keep them as they are (small ratio).
        min = metrics.min, // min and max are not adjusted
        max = metrics.max, // min and max are not adjusted
        sum = DoubleVectorValue(DoubleArray(metrics.logicalSize) { (metrics.sum.data[it] * factor) })
    )

    /** The arithmetic mean for the values seen by this [DoubleVectorValueStatistics]. */
    override val mean: DoubleVectorValue
        get() = DoubleVectorValue(DoubleArray(this.type.logicalSize) {
            this.sum[it].value / this.numberOfNonNullEntries
        })

    /**
     * Xodus serializer for [DoubleVectorValueStatistics]
     */
    class Binding(val logicalSize: Int): MetricsXodusBinding<DoubleVectorValueStatistics> {
        override fun read(stream: ByteArrayInputStream): DoubleVectorValueStatistics {
            val stat = DoubleVectorValueStatistics(this@Binding.logicalSize)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfDistinctEntries = LongBinding.readCompressed(stream)
            for (i in 0 until this@Binding.logicalSize) {
                stat.min.data[i] = SignedDoubleBinding.BINDING.readObject(stream)
                stat.max.data[i] = SignedDoubleBinding.BINDING.readObject(stream)
                stat.sum.data[i] = SignedDoubleBinding.BINDING.readObject(stream)
            }
            return stat
        }

        override fun write(output: LightOutputStream, statistics: DoubleVectorValueStatistics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
            for (i in 0 until statistics.type.logicalSize) {
                SignedDoubleBinding.BINDING.writeObject(output, statistics.min.data[i])
                SignedDoubleBinding.BINDING.writeObject(output, statistics.max.data[i])
                SignedDoubleBinding.BINDING.writeObject(output, statistics.sum.data[i])
            }
        }
    }

}