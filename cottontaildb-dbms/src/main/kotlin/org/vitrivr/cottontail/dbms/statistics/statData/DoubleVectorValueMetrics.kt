package org.vitrivr.cottontail.dbms.statistics.statData

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream

/**
 * A [DataMetrics] implementation for [DoubleVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class DoubleVectorValueMetrics(logicalSize: Int) : RealVectorValueMetrics<DoubleVectorValue>(Types.DoubleVector(logicalSize)) {
    /** Minimum value seen by this [DoubleVectorValueMetrics]. */
    override val min: DoubleVectorValue = DoubleVectorValue(DoubleArray(this.type.logicalSize) { Double.MAX_VALUE })

    /** Minimum value seen by this [DoubleVectorValueMetrics]. */
    override val max: DoubleVectorValue = DoubleVectorValue(DoubleArray(this.type.logicalSize) { Double.MIN_VALUE })

    /** Sum of all floats values seen by this [DoubleVectorValueMetrics]. */
    override val sum: DoubleVectorValue = DoubleVectorValue(DoubleArray(this.type.logicalSize))

    /** The arithmetic mean for the values seen by this [DoubleVectorValueMetrics]. */
    override val mean: DoubleVectorValue
        get() = DoubleVectorValue(DoubleArray(this.type.logicalSize) {
            this.sum[it].value / this.numberOfNonNullEntries
        })

    /**
     * Xodus serializer for [DoubleVectorValueMetrics]
     */
    class Binding(val logicalSize: Int): MetricsXodusBinding<DoubleVectorValueMetrics> {
        override fun read(stream: ByteArrayInputStream): DoubleVectorValueMetrics {
            val stat = DoubleVectorValueMetrics(this.logicalSize)
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            for (i in 0 until this.logicalSize) {
                stat.min.data[i] = SignedDoubleBinding.BINDING.readObject(stream)
                stat.max.data[i] = SignedDoubleBinding.BINDING.readObject(stream)
                stat.sum.data[i] = SignedDoubleBinding.BINDING.readObject(stream)
            }
            return stat
        }

        override fun write(output: LightOutputStream, statistics: DoubleVectorValueMetrics) {
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            for (i in 0 until statistics.type.logicalSize) {
                SignedDoubleBinding.BINDING.writeObject(output, statistics.min.data[i])
                SignedDoubleBinding.BINDING.writeObject(output, statistics.max.data[i])
                SignedDoubleBinding.BINDING.writeObject(output, statistics.sum.data[i])
            }
        }
    }

    /**
     * Resets this [DoubleVectorValueMetrics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        for (i in 0 until this.type.logicalSize) {
            this.min.data[i] = Double.MAX_VALUE
            this.max.data[i] = Double.MIN_VALUE
            this.sum.data[i] = 0.0
        }
    }

}