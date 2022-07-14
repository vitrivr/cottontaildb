package org.vitrivr.cottontail.dbms.statistics.columns

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.LongVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream
import java.lang.Long.max
import java.lang.Long.min

/**
 * A [ValueStatistics] implementation for [LongVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class LongVectorValueStatistics(logicalSize: Int): RealVectorValueStatistics<LongVectorValue>(Types.LongVector(logicalSize)) {
    /** Minimum value seen by this [LongVectorValueStatistics]. */
    override val min: LongVectorValue = LongVectorValue(LongArray(this.type.logicalSize) { Long.MAX_VALUE })

    /** Minimum value seen by this [LongVectorValueStatistics]. */
    override val max: LongVectorValue = LongVectorValue(LongArray(this.type.logicalSize) { Long.MIN_VALUE })

    /** Sum of all values seen by this [IntVectorValueStatistics]. */
    override val sum: LongVectorValue = LongVectorValue(LongArray(this.type.logicalSize))

    /** The arithmetic for the values seen by this [DoubleVectorValueStatistics]. */
    override val mean: LongVectorValue
        get() = LongVectorValue(LongArray(this.type.logicalSize) {
            (this.sum[it].value / this.numberOfNonNullEntries)
        })

    /**
     * Xodus serializer for [LongVectorValueStatistics]
     */
    class Binding(val logicalSize: Int): XodusBinding<LongVectorValueStatistics> {
        override fun read(stream: ByteArrayInputStream): LongVectorValueStatistics {
            val stat = LongVectorValueStatistics(this.logicalSize)
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            for (i in 0 until this.logicalSize) {
                stat.min.data[i] = LongBinding.BINDING.readObject(stream)
                stat.max.data[i] = LongBinding.BINDING.readObject(stream)
                stat.sum.data[i] = LongBinding.BINDING.readObject(stream)
            }
            return stat
        }

        override fun write(output: LightOutputStream, statistics: LongVectorValueStatistics) {
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            for (i in 0 until statistics.type.logicalSize) {
                LongBinding.BINDING.writeObject(output, statistics.min.data[i])
                LongBinding.BINDING.writeObject(output, statistics.max.data[i])
                LongBinding.BINDING.writeObject(output, statistics.sum.data[i])
            }
        }
    }

    /**
     * Updates this [LongVectorValueStatistics] with an inserted [LongVectorValue]
     *
     * @param inserted The [LongVectorValue] that was inserted.
     */
    override fun insert(inserted: LongVectorValue?) {
        super.insert(inserted)
        if (inserted != null) {
            for ((i, d) in inserted.data.withIndex()) {
                this.min.data[i] = min(d, this.min.data[i])
                this.max.data[i] = max(d, this.max.data[i])
            }
        }
    }

    /**
     * Updates this [LongVectorValueStatistics] with a deleted [LongVectorValue]
     *
     * @param deleted The [LongVectorValue] that was deleted.
     */
    override fun delete(deleted: LongVectorValue?) {
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
     * Resets this [LongVectorValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        for (i in 0 until this.type.logicalSize) {
            this.min.data[i] = Long.MAX_VALUE
            this.max.data[i] = Long.MIN_VALUE
        }
    }

    /**
     * Copies this [LongVectorValueStatistics] and returns it.
     *
     * @return Copy of this [LongVectorValueStatistics].
     */
    override fun copy(): LongVectorValueStatistics {
        val copy = LongVectorValueStatistics(this.type.logicalSize)
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