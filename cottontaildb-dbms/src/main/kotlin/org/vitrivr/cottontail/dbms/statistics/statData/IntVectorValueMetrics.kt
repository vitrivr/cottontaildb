package org.vitrivr.cottontail.dbms.statistics.statData

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.IntVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream
import java.lang.Integer.max
import java.lang.Integer.min

/**
 * A [DataMetrics] implementation for [IntVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class IntVectorValueMetrics(logicalSize: Int): RealVectorValueMetrics<IntVectorValue>(Types.IntVector(logicalSize)) {
    /** Minimum value seen by this [IntVectorValueMetrics]. */
    override val min: IntVectorValue = IntVectorValue(IntArray(this.type.logicalSize) { Int.MAX_VALUE })

    /** Minimum value seen by this [IntVectorValueMetrics]. */
    override val max: IntVectorValue = IntVectorValue(IntArray(this.type.logicalSize) { Int.MIN_VALUE })

    /** Sum of all values seen by this [IntVectorValueMetrics]. */
    override val sum: IntVectorValue = IntVectorValue(IntArray(this.type.logicalSize))

    /** The arithmetic for the values seen by this [DoubleVectorValueMetrics]. */
    override val mean: IntVectorValue
        get() = IntVectorValue(IntArray(this.type.logicalSize) {
            (this.sum[it].value / this.numberOfNonNullEntries).toInt()
        })

    /**
     * Xodus serializer for [IntVectorValueMetrics]
     */
    class Binding(val logicalSize: Int): MetricsXodusBinding<IntVectorValueMetrics> {
        override fun read(stream: ByteArrayInputStream): IntVectorValueMetrics {
            val stat = IntVectorValueMetrics(this.logicalSize)
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            for (i in 0 until this.logicalSize) {
                stat.min.data[i] = IntegerBinding.BINDING.readObject(stream)
                stat.max.data[i] = IntegerBinding.BINDING.readObject(stream)
                stat.sum.data[i] = IntegerBinding.BINDING.readObject(stream)

            }
            return stat
        }

        override fun write(output: LightOutputStream, statistics: IntVectorValueMetrics) {
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            for (i in 0 until statistics.type.logicalSize) {
                IntegerBinding.BINDING.writeObject(output, statistics.min.data[i])
                IntegerBinding.BINDING.writeObject(output, statistics.max.data[i])
                IntegerBinding.BINDING.writeObject(output, statistics.sum.data[i])
            }
        }
    }

    /**
     * Resets this [IntVectorValueMetrics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        for (i in 0 until this.type.logicalSize) {
            this.min.data[i] = Int.MAX_VALUE
            this.max.data[i] = Int.MIN_VALUE
        }
    }

}