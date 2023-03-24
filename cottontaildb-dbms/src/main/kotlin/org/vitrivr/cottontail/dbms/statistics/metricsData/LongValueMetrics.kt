package org.vitrivr.cottontail.dbms.statistics.metricsData

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueMetrics] implementation for [LongValue]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class LongValueMetrics : RealValueMetrics<LongValue>(Types.Long) {

    /**
     * Xodus serializer for [LongValueMetrics]
     */
    object Binding: MetricsXodusBinding<LongValueMetrics> {
        override fun read(stream: ByteArrayInputStream): LongValueMetrics {
            val stat = LongValueMetrics()
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.min = LongValue(LongBinding.BINDING.readObject(stream))
            stat.max = LongValue(LongBinding.BINDING.readObject(stream))
            stat.sum = DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
            return stat
        }

        override fun write(output: LightOutputStream, statistics: LongValueMetrics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.BINDING.writeObject(output, statistics.min.value)
            LongBinding.BINDING.writeObject(output, statistics.max.value)
            SignedDoubleBinding.BINDING.writeObject(output, statistics.sum.value)
        }
    }

    /** Minimum value seen by this [LongValueMetrics]. */
    override var min: LongValue = LongValue.MAX_VALUE
        private set

    /** Minimum value seen by this [LongValueMetrics]. */
    override var max: LongValue = LongValue.MIN_VALUE
        private set

    /** Sum of all [LongValue]s seen by this [LongValueMetrics]. */
    override var sum: DoubleValue = DoubleValue.ZERO
        private set

    /**
     * Resets this [LongValueMetrics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = LongValue.MAX_VALUE
        this.max = LongValue.MIN_VALUE
    }
}