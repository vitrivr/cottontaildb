package org.vitrivr.cottontail.dbms.statistics.metricsData

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedFloatBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueMetrics] implementation for [FloatVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
data class FloatVectorValueMetrics(
    val logicalSize: Int,
    override var numberOfNullEntries: Long = 0L,
    override var numberOfNonNullEntries: Long = 0L,
    override var numberOfDistinctEntries: Long = 0L,
    override val min: FloatVectorValue = FloatVectorValue(FloatArray(logicalSize) { Float.MAX_VALUE }),
    override val max: FloatVectorValue = FloatVectorValue(FloatArray(logicalSize) { Float.MIN_VALUE }),
    override val sum: FloatVectorValue = FloatVectorValue(FloatArray(logicalSize))
) : RealVectorValueMetrics<FloatVectorValue>(Types.FloatVector(logicalSize)) {

    /**
     * Constructor for the collector to get from the sample to the population
     */
    constructor(factor: Float, metrics: FloatVectorValueMetrics): this(
        logicalSize = metrics.logicalSize,
        numberOfNullEntries = (metrics.numberOfNullEntries * factor).toLong(),
        numberOfNonNullEntries = (metrics.numberOfNonNullEntries * factor).toLong(),
        numberOfDistinctEntries = (metrics.numberOfDistinctEntries * factor).toLong(),
        min = metrics.min, // min and max are not adjusted
        max = metrics.max, // min and max are not adjusted
        sum = FloatVectorValue(FloatArray(metrics.logicalSize) { (metrics.sum.data[it] * factor) })
    )

    /** The arithmetic for the values seen by this [DoubleVectorValueMetrics]. */
    override val mean: FloatVectorValue
        get() = FloatVectorValue(FloatArray(this.type.logicalSize) {
            this.sum[it].value / this.numberOfNonNullEntries
        })

    /**
     * Xodus serializer for [FloatVectorValueMetrics]
     */
    class Binding(val logicalSize: Int): MetricsXodusBinding<FloatVectorValueMetrics> {
        override fun read(stream: ByteArrayInputStream): FloatVectorValueMetrics {
            val stat = FloatVectorValueMetrics(this@Binding.logicalSize)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfDistinctEntries = LongBinding.readCompressed(stream)
            for (i in 0 until this@Binding.logicalSize) {
                stat.min.data[i] = SignedFloatBinding.BINDING.readObject(stream)
                stat.max.data[i] = SignedFloatBinding.BINDING.readObject(stream)
                stat.sum.data[i] = SignedFloatBinding.BINDING.readObject(stream)
            }
            return stat
        }

        override fun write(output: LightOutputStream, statistics: FloatVectorValueMetrics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
            for (i in 0 until statistics.type.logicalSize) {
                SignedFloatBinding.BINDING.writeObject(output, statistics.min.data[i])
                SignedFloatBinding.BINDING.writeObject(output, statistics.max.data[i])
                SignedFloatBinding.BINDING.writeObject(output, statistics.sum.data[i])
            }
        }
    }

}