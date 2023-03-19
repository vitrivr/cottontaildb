package org.vitrivr.cottontail.dbms.statistics.statData

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream
import java.lang.Integer.max
import java.lang.Integer.min

/**
 * A [DataMetrics] implementation for [IntValue]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class IntValueMetrics : RealValueMetrics<IntValue>(Types.Int) {

    /**
     * Xodus serializer for [IntValueMetrics]
     */
    object Binding : MetricsXodusBinding<IntValueMetrics> {
        override fun read(stream: ByteArrayInputStream): IntValueMetrics {
            val stat = IntValueMetrics()
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.min = IntValue(IntegerBinding.BINDING.readObject(stream))
            stat.max = IntValue(IntegerBinding.BINDING.readObject(stream))
            stat.sum = DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
            return stat
        }

        override fun write(output: LightOutputStream, statistics: IntValueMetrics) {
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            IntegerBinding.BINDING.writeObject(output, statistics.min.value)
            IntegerBinding.BINDING.writeObject(output, statistics.max.value)
            SignedDoubleBinding.BINDING.writeObject(output, statistics.sum.value)
        }
    }

    /** Minimum value seen by this [IntValueMetrics]. */
    override var min: IntValue = IntValue.MAX_VALUE
        private set

    /** Minimum value seen by this [IntValueMetrics]. */
    override var max: IntValue = IntValue.MIN_VALUE
        private set

    /** Sum of all [IntValue]s seen by this [IntValueMetrics]. */
    override var sum: DoubleValue = DoubleValue.ZERO
        private set

    /**
     * Resets this [IntValueMetrics] and sets all its values to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = IntValue.MAX_VALUE
        this.max = IntValue.MIN_VALUE
    }
}
