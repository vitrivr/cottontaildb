package org.vitrivr.cottontail.dbms.statistics.statData

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.bindings.SignedFloatBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.FloatValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream
import java.lang.Float.max
import java.lang.Float.min

/**
 * A [DataMetrics] implementation for [FloatValue]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class FloatValueMetrics : RealValueMetrics<FloatValue>(Types.Float) {

    /**
     * Xodus serializer for [FloatValueMetrics]
     */
    object Binding: MetricsXodusBinding<FloatValueMetrics> {
        override fun read(stream: ByteArrayInputStream): FloatValueMetrics {
            val stat = FloatValueMetrics()
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.min = FloatValue(SignedFloatBinding.BINDING.readObject(stream))
            stat.max = FloatValue(SignedFloatBinding.BINDING.readObject(stream))
            stat.sum = DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
            return stat
        }

        override fun write(output: LightOutputStream, statistics: FloatValueMetrics) {
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            SignedFloatBinding.BINDING.writeObject(output, statistics.min.value)
            SignedFloatBinding.BINDING.writeObject(output, statistics.max.value)
            SignedDoubleBinding.BINDING.writeObject(output, statistics.sum.value)
        }
    }

    /** Minimum value seen by this [FloatValueMetrics]. */
    override var min: FloatValue = FloatValue.MAX_VALUE
        private set

    /** Minimum value seen by this [FloatValueMetrics]. */
    override var max: FloatValue = FloatValue.MIN_VALUE
        private set

    /** Sum of all [DoubleValue]s seen by this [FloatValueMetrics]. */
    override var sum: DoubleValue = DoubleValue.ZERO
        private set

    /**
     * Resets this [FloatValueMetrics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = FloatValue.MAX_VALUE
        this.max = FloatValue.MIN_VALUE
        this.sum = DoubleValue.ZERO
    }
}