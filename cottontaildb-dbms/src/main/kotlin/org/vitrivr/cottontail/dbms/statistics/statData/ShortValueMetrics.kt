package org.vitrivr.cottontail.dbms.statistics.statData

import com.google.common.primitives.Shorts.max
import com.google.common.primitives.Shorts.min
import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.ShortBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.ShortValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream

/**
 * A [DataMetrics] implementation for [ShortValue]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class ShortValueMetrics : RealValueMetrics<ShortValue>(Types.Short) {

    /**
     * Xodus serializer for [ShortValueMetrics]
     */
    object Binding: MetricsXodusBinding<ShortValueMetrics> {
        override fun read(stream: ByteArrayInputStream): ShortValueMetrics {
            val stat = ShortValueMetrics()
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.min = ShortValue(ShortBinding.BINDING.readObject(stream))
            stat.max = ShortValue(ShortBinding.BINDING.readObject(stream))
            return stat
        }

        override fun write(output: LightOutputStream, statistics: ShortValueMetrics) {
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            ShortBinding.BINDING.writeObject(output, statistics.min.value)
            ShortBinding.BINDING.writeObject(output, statistics.max.value)
        }
    }

    /** Minimum value seen by this [ShortValueMetrics]. */
    override var min: ShortValue = ShortValue.MAX_VALUE
        private set

    /** Minimum value seen by this [ShortValueMetrics]. */
    override var max: ShortValue = ShortValue.MIN_VALUE
        private set

    /** Sum of all [IntValue]s seen by this [ShortValueMetrics]. */
    override var sum: DoubleValue = DoubleValue.ZERO
        private set

    /**
     * Resets this [ShortValueMetrics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = ShortValue.MAX_VALUE
        this.max = ShortValue.MIN_VALUE
        this.sum = DoubleValue.ZERO
    }

}