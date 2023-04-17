package org.vitrivr.cottontail.dbms.statistics.metricsData

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.IntVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueMetrics] implementation for [IntVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
data class IntVectorValueMetrics(
    val logicalSize: Int,
    override var numberOfNullEntries: Long = 0L,
    override var numberOfNonNullEntries: Long = 0L,
    override var numberOfDistinctEntries: Long = 0L,
    override val min: IntVectorValue = IntVectorValue(IntArray(logicalSize) { Int.MAX_VALUE }),
    override val max: IntVectorValue = IntVectorValue(IntArray(logicalSize) { Int.MIN_VALUE }),
    override val sum: IntVectorValue = IntVectorValue(IntArray(logicalSize))
): RealVectorValueMetrics<IntVectorValue>(Types.IntVector(logicalSize)) {

    /**
     * Constructor for the collector to get from the sample to the population
     */
    constructor(factor: Float, metrics: IntVectorValueMetrics): this(
        logicalSize = metrics.logicalSize,
        numberOfNullEntries = (metrics.numberOfNullEntries * factor).toLong(),
        numberOfNonNullEntries = (metrics.numberOfNonNullEntries * factor).toLong(),
        numberOfDistinctEntries = (metrics.numberOfDistinctEntries * factor).toLong(),
        min = metrics.min, // min and max are not adjusted
        max = metrics.max, // min and max are not adjusted
        sum = IntVectorValue(IntArray(metrics.logicalSize) { (metrics.sum.data[it] * factor).toInt() })
    )

    /** The arithmetic for the values seen by this [DoubleVectorValueMetrics]. */
    override val mean: IntVectorValue
        get() = IntVectorValue(IntArray(this.type.logicalSize) {
            (this.sum[it].value / this.numberOfNonNullEntries).toInt()
        })

    /**
     * Xodus serializer for [IntVectorValueMetrics]
     */
    class Binding(val logicalSize: Int): MetricsXodusBinding<IntVectorValueMetrics> {
        override fun read(stream: ByteArrayInputStream): IntVectorValueMetrics {
            val stat = IntVectorValueMetrics(this@Binding.logicalSize)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfDistinctEntries = LongBinding.readCompressed(stream)
            for (i in 0 until this@Binding.logicalSize) {
                stat.min.data[i] = IntegerBinding.BINDING.readObject(stream)
                stat.max.data[i] = IntegerBinding.BINDING.readObject(stream)
                stat.sum.data[i] = IntegerBinding.BINDING.readObject(stream)
            }
            return stat
        }

        override fun write(output: LightOutputStream, statistics: IntVectorValueMetrics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
            for (i in 0 until statistics.type.logicalSize) {
                IntegerBinding.BINDING.writeObject(output, statistics.min.data[i])
                IntegerBinding.BINDING.writeObject(output, statistics.max.data[i])
                IntegerBinding.BINDING.writeObject(output, statistics.sum.data[i])
            }
        }
    }

    /**
     * Resets this [IntVectorValueMetrics] and sets all its values to the default value.
     */
    override fun reset() {
        super.reset()
        for (i in 0 until this.type.logicalSize) {
            this.min.data[i] = Int.MAX_VALUE
            this.max.data[i] = Int.MIN_VALUE
        }
    }

}