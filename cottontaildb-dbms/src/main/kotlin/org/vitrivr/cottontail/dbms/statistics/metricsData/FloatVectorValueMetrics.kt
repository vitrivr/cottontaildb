package org.vitrivr.cottontail.dbms.statistics.metricsData

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedFloatBinding
import jetbrains.exodus.util.LightOutputStream
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
class FloatVectorValueMetrics(logicalSize: Int) : RealVectorValueMetrics<FloatVectorValue>(Types.FloatVector(logicalSize)) {
    /** Minimum value in this [FloatVectorValueMetrics]. */
    override val min: FloatVectorValue = FloatVectorValue(FloatArray(this.type.logicalSize) { Float.MAX_VALUE })

    /** Minimum value in this [FloatVectorValueMetrics]. */
    override val max: FloatVectorValue = FloatVectorValue(FloatArray(this.type.logicalSize) { Float.MIN_VALUE })

    /** Sum of all floats values in this [FloatVectorValueMetrics]. */
    override val sum: FloatVectorValue = FloatVectorValue(FloatArray(this.type.logicalSize))

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
            val stat = FloatVectorValueMetrics(logicalSize)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            for (i in 0 until this.logicalSize) {
                stat.min.data[i] = SignedFloatBinding.BINDING.readObject(stream)
                stat.max.data[i] = SignedFloatBinding.BINDING.readObject(stream)
                stat.sum.data[i] = SignedFloatBinding.BINDING.readObject(stream)
            }
            return stat
        }

        override fun write(output: LightOutputStream, statistics: FloatVectorValueMetrics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            for (i in 0 until statistics.type.logicalSize) {
                SignedFloatBinding.BINDING.writeObject(output, statistics.min.data[i])
                SignedFloatBinding.BINDING.writeObject(output, statistics.max.data[i])
                SignedFloatBinding.BINDING.writeObject(output, statistics.sum.data[i])
            }
        }
    }

    /**
     * Resets this [FloatVectorValueMetrics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        for (i in 0 until this.type.logicalSize) {
            this.min.data[i] = Float.MAX_VALUE
            this.max.data[i] = Float.MIN_VALUE
            this.sum.data[i] = 0.0f
        }
    }
}