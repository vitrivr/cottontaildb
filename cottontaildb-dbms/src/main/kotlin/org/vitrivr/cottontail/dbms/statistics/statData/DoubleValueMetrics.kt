package org.vitrivr.cottontail.dbms.statistics.statData

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream
import java.lang.Double.max
import java.lang.Double.min

/**
 * A [DataMetrics] implementation for [DoubleValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class DoubleValueMetrics : RealValueMetrics<DoubleValue>(Types.Double) {

    /**
     * Xodus serializer for [DoubleValueMetrics]
     */
    object Binding: MetricsXodusBinding<DoubleValueMetrics> {
        override fun read(stream: ByteArrayInputStream): DoubleValueMetrics {
            val stat = DoubleValueMetrics()
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.min = DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
            stat.max = DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
            stat.sum = DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
            return stat
        }

        override fun write(output: LightOutputStream, statistics: DoubleValueMetrics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            SignedDoubleBinding.BINDING.writeObject(output, statistics.min.value)
            SignedDoubleBinding.BINDING.writeObject(output, statistics.max.value)
            SignedDoubleBinding.BINDING.writeObject(output, statistics.sum.value)
        }
    }

    /** Minimum value in this [DoubleValueMetrics]. */
    override var min: DoubleValue = DoubleValue.MAX_VALUE
        private set

    /** Minimum value in this [DoubleValueMetrics]. */
    override var max: DoubleValue = DoubleValue.MAX_VALUE
        private set

    /** Sum of all floats values in this [DoubleValueMetrics]. */
    override var sum: DoubleValue = DoubleValue.ZERO
        private set

    /**
     * Resets this [DoubleValueMetrics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = DoubleValue.MAX_VALUE
        this.max = DoubleValue.MIN_VALUE
        this.sum = DoubleValue.ZERO
    }
}