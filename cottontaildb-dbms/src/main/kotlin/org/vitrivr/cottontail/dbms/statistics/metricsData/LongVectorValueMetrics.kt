package org.vitrivr.cottontail.dbms.statistics.metricsData

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.LongVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueMetrics] implementation for [LongVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class LongVectorValueMetrics(logicalSize: Int): RealVectorValueMetrics<LongVectorValue>(Types.LongVector(logicalSize)) {
    /** Minimum value seen by this [LongVectorValueMetrics]. */
    override val min: LongVectorValue = LongVectorValue(LongArray(this.type.logicalSize) { Long.MAX_VALUE })

    /** Minimum value seen by this [LongVectorValueMetrics]. */
    override val max: LongVectorValue = LongVectorValue(LongArray(this.type.logicalSize) { Long.MIN_VALUE })

    /** Sum of all values seen by this [IntVectorValueMetrics]. */
    override val sum: LongVectorValue = LongVectorValue(LongArray(this.type.logicalSize))

    /** The arithmetic for the values seen by this [DoubleVectorValueMetrics]. */
    override val mean: LongVectorValue
        get() = LongVectorValue(LongArray(this.type.logicalSize) {
            (this.sum[it].value / this.numberOfNonNullEntries)
        })

    /**
     * Xodus serializer for [LongVectorValueMetrics]
     */
    class Binding(val logicalSize: Int): MetricsXodusBinding<LongVectorValueMetrics> {
        override fun read(stream: ByteArrayInputStream): LongVectorValueMetrics {
            val stat = LongVectorValueMetrics(this.logicalSize)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            for (i in 0 until this.logicalSize) {
                stat.min.data[i] = LongBinding.BINDING.readObject(stream)
                stat.max.data[i] = LongBinding.BINDING.readObject(stream)
                stat.sum.data[i] = LongBinding.BINDING.readObject(stream)
            }
            return stat
        }

        override fun write(output: LightOutputStream, statistics: LongVectorValueMetrics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            for (i in 0 until statistics.type.logicalSize) {
                LongBinding.BINDING.writeObject(output, statistics.min.data[i])
                LongBinding.BINDING.writeObject(output, statistics.max.data[i])
                LongBinding.BINDING.writeObject(output, statistics.sum.data[i])
            }
        }
    }

    /**
     * Resets this [LongVectorValueMetrics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        for (i in 0 until this.type.logicalSize) {
            this.min.data[i] = Long.MAX_VALUE
            this.max.data[i] = Long.MIN_VALUE
        }
    }

}