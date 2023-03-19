package org.vitrivr.cottontail.dbms.statistics.statData

import com.google.common.primitives.SignedBytes.max
import com.google.common.primitives.SignedBytes.min
import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.ByteBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.ByteValue
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream

/**
 * A [DataMetrics] implementation for [ByteValue]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class ByteValueMetrics : RealValueMetrics<ByteValue>(Types.Byte) {

    /**
     * Xodus serializer for [ByteValueMetrics]
     */
    object Binding: MetricsXodusBinding<ByteValueMetrics> {
        override fun read(stream: ByteArrayInputStream): ByteValueMetrics {
            val stat = ByteValueMetrics()
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.min = ByteValue(ByteBinding.BINDING.readObject(stream))
            stat.max = ByteValue(ByteBinding.BINDING.readObject(stream))
            return stat
        }

        override fun write(output: LightOutputStream, statistics: ByteValueMetrics) {
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            ByteBinding.BINDING.writeObject(output, statistics.min.value)
            ByteBinding.BINDING.writeObject(output, statistics.max.value)
        }
    }

    /** Minimum value seen by this [ByteValueMetrics]. */
    override var min: ByteValue = ByteValue.MAX_VALUE
        private set

    /** Minimum value seen by this [ByteValueMetrics]. */
    override var max: ByteValue = ByteValue.MIN_VALUE
        private set

    /** Sum of all [ByteValue]s seen by this [ByteValueMetrics]. */
    override var sum: DoubleValue = DoubleValue.ZERO
        private set

    /**
     * Resets this [ByteValueMetrics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = ByteValue.MAX_VALUE
        this.max = ByteValue.MIN_VALUE
        this.sum = DoubleValue.ZERO
    }

}