package org.vitrivr.cottontail.dbms.statistics.values

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.LongVectorValue
import org.vitrivr.cottontail.storage.serializers.statistics.MetricsXodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueStatistics] implementation for [LongVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class LongVectorValueStatistics(
    val logicalSize: Int,
    override var numberOfNullEntries: Long = 0L,
    override var numberOfNonNullEntries: Long = 0L,
    override var numberOfDistinctEntries: Long = 0L,
    override val min: LongVectorValue = LongVectorValue(LongArray(logicalSize) { Long.MAX_VALUE }),
    override val max: LongVectorValue = LongVectorValue(LongArray(logicalSize) { Long.MIN_VALUE }),
    override val sum: LongVectorValue = LongVectorValue(LongArray(logicalSize))
): RealVectorValueStatistics<LongVectorValue>(Types.LongVector(logicalSize)) {

    /**
     * Constructor for the collector to get from the sample to the population
     */
    constructor(factor: Float, metrics: LongVectorValueStatistics): this(
        logicalSize = metrics.logicalSize,
        numberOfNullEntries = (metrics.numberOfNullEntries * factor).toLong(),
        numberOfNonNullEntries = (metrics.numberOfNonNullEntries * factor).toLong(),
        numberOfDistinctEntries = if (metrics.numberOfDistinctEntries.toDouble() / metrics.numberOfEntries.toDouble() >= metrics.distinctEntriesScalingThreshold) (metrics.numberOfDistinctEntries * factor).toLong() else metrics.numberOfDistinctEntries, // Depending on the ratio between distinct entries and number of entries, we either scale the distinct entries (large ratio) or keep them as they are (small ratio).
        min = metrics.min, // min and max are not adjusted
        max = metrics.max, // min and max are not adjusted
        sum = LongVectorValue(LongArray(metrics.logicalSize) { (metrics.sum.data[it] * factor).toLong() })
    )

    /** The arithmetic for the values seen by this [DoubleVectorValueStatistics]. */
    override val mean: LongVectorValue
        get() = LongVectorValue(LongArray(this.type.logicalSize) {
            (this.sum[it].value / this.numberOfNonNullEntries)
        })

    /**
     * Xodus serializer for [LongVectorValueStatistics]
     */
    class Binding(val logicalSize: Int): MetricsXodusBinding<LongVectorValueStatistics> {
        override fun read(stream: ByteArrayInputStream): LongVectorValueStatistics {
            val stat = LongVectorValueStatistics(this@Binding.logicalSize)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfDistinctEntries = LongBinding.readCompressed(stream)
            for (i in 0 until this@Binding.logicalSize) {
                stat.min.data[i] = LongBinding.BINDING.readObject(stream)
                stat.max.data[i] = LongBinding.BINDING.readObject(stream)
                stat.sum.data[i] = LongBinding.BINDING.readObject(stream)
            }
            return stat
        }

        override fun write(output: LightOutputStream, statistics: LongVectorValueStatistics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
            for (i in 0 until statistics.type.logicalSize) {
                LongBinding.BINDING.writeObject(output, statistics.min.data[i])
                LongBinding.BINDING.writeObject(output, statistics.max.data[i])
                LongBinding.BINDING.writeObject(output, statistics.sum.data[i])
            }
        }
    }

}