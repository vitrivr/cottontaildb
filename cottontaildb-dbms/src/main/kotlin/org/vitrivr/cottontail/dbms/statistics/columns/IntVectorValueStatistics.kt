package org.vitrivr.cottontail.dbms.statistics.columns

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.IntVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream
import java.lang.Integer.max
import java.lang.Integer.min

/**
 * A [ValueStatistics] implementation for [IntVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class IntVectorValueStatistics(logicalSize: Int) : AbstractValueStatistics<IntVectorValue>(Types.IntVector(logicalSize)), VectorValueStatistics<IntVectorValue> {
    /** Minimum value seen by this [IntVectorValueStatistics]. */
    override val min: IntVectorValue = IntVectorValue(IntArray(this.type.logicalSize) { Int.MAX_VALUE })

    /** Minimum value seen by this [IntVectorValueStatistics]. */
    override val max: IntVectorValue = IntVectorValue(IntArray(this.type.logicalSize) { Int.MIN_VALUE })

    /** Sum of all values seen by this [IntVectorValueStatistics]. */
    override val sum: IntVectorValue = IntVectorValue(IntArray(this.type.logicalSize))

    /** The arithmetic for the values seen by this [DoubleVectorValueStatistics]. */
    override val mean: IntVectorValue
        get() = IntVectorValue(IntArray(this.type.logicalSize) {
            (this.sum[it].value / this.numberOfNonNullEntries).toInt()
        })

    /**
     * Xodus serializer for [IntVectorValueStatistics]
     */
    class Binding(val logicalSize: Int): XodusBinding<IntVectorValueStatistics> {
        override fun read(stream: ByteArrayInputStream): IntVectorValueStatistics {
            val stat = IntVectorValueStatistics(this.logicalSize)
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

        override fun write(output: LightOutputStream, statistics: IntVectorValueStatistics) {
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
     * Updates this [IntVectorValueStatistics] with an inserted [IntVectorValue]
     *
     * @param inserted The [IntVectorValue] that was inserted.
     */
    override fun insert(inserted: IntVectorValue?) {
        super.insert(inserted)
        if (inserted != null) {
            for ((i, d) in inserted.data.withIndex()) {
                this.min.data[i] = min(d, this.min.data[i])
                this.max.data[i] = max(d, this.max.data[i])
            }
        }
    }

    /**
     * Updates this [IntVectorValueStatistics] with a deleted [IntVectorValue]
     *
     * @param deleted The [IntVectorValue] that was deleted.
     */
    override fun delete(deleted: IntVectorValue?) {
        super.delete(deleted)
        if (deleted != null) {
            for ((i, d) in deleted.data.withIndex()) {
                if (this.min.data[i] == d || this.max.data[i] == d) {
                    this.fresh = false
                }
            }
        }
    }

    /**
     * Resets this [IntVectorValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        for (i in 0 until this.type.logicalSize) {
            this.min.data[i] = Int.MAX_VALUE
            this.max.data[i] = Int.MIN_VALUE
        }
    }

    /**
     * Copies this [IntVectorValueStatistics] and returns it.
     *
     * @return Copy of this [IntVectorValueStatistics].
     */
    override fun copy(): IntVectorValueStatistics {
        val copy = IntVectorValueStatistics(this.type.logicalSize)
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        for (i in 0 until this.type.logicalSize) {
            copy.min.data[i] = this.min.data[i]
            copy.max.data[i] = this.max.data[i]
            copy.sum.data[i] = this.sum.data[i]
        }
        return copy
    }
}