package org.vitrivr.cottontail.dbms.statistics.columns

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueStatistics] implementation for [DoubleVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class DoubleVectorValueStatistics(logicalSize: Int) : AbstractValueStatistics<DoubleVectorValue>(Types.DoubleVector(logicalSize)), VectorValueStatistics<DoubleVectorValue> {
    /** Minimum value seen by this [DoubleVectorValueStatistics]. */
    override val min: DoubleVectorValue = DoubleVectorValue(DoubleArray(this.type.logicalSize) { Double.MAX_VALUE })

    /** Minimum value seen by this [DoubleVectorValueStatistics]. */
    override val max: DoubleVectorValue = DoubleVectorValue(DoubleArray(this.type.logicalSize) { Double.MIN_VALUE })

    /** Sum of all floats values seen by this [DoubleVectorValueStatistics]. */
    override val sum: DoubleVectorValue = DoubleVectorValue(DoubleArray(this.type.logicalSize))

    /** The arithmetic mean for the values seen by this [DoubleVectorValueStatistics]. */
    override val mean: DoubleVectorValue
        get() = DoubleVectorValue(DoubleArray(this.type.logicalSize) {
            this.sum[it].value / this.numberOfNonNullEntries
        })

    /**
     * Xodus serializer for [DoubleVectorValueStatistics]
     */
    class Binding(val logicalSize: Int): XodusBinding<DoubleVectorValueStatistics> {
        override fun read(stream: ByteArrayInputStream): DoubleVectorValueStatistics {
            val stat = DoubleVectorValueStatistics(this.logicalSize)
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

        override fun write(output: LightOutputStream, statistics: DoubleVectorValueStatistics) {
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
     * Updates this [DoubleVectorValueStatistics] with an inserted [DoubleVectorValue]
     *
     * @param inserted The [DoubleVectorValue] that was inserted.
     */
    override fun insert(inserted: DoubleVectorValue?) {
        super.insert(inserted)
        if (inserted != null) {
            for ((i, d) in inserted.data.withIndex()) {
                this.min.data[i] = java.lang.Double.min(d, this.min.data[i])
                this.max.data[i] = java.lang.Double.max(d, this.max.data[i])
                this.sum.data[i] += d
            }
        }
    }

    /**
     * Updates this [DoubleVectorValueStatistics] with a deleted [DoubleVectorValue]
     *
     * @param deleted The [DoubleVectorValue] that was deleted.
     */
    override fun delete(deleted: DoubleVectorValue?) {
        super.delete(deleted)
        if (deleted != null) {
            for ((i, d) in deleted.data.withIndex()) {
                /* We cannot create a sensible estimate if a value is deleted. */
                if (this.min.data[i] == d || this.max.data[i] == d) {
                    this.fresh = false
                }
                this.sum.data[i] -= d
            }
        }
    }

    /**
     * Resets this [DoubleVectorValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        for (i in 0 until this.type.logicalSize) {
            this.min.data[i] = Double.MAX_VALUE
            this.max.data[i] = Double.MIN_VALUE
            this.sum.data[i] = 0.0
        }
    }

    /**
     * Copies this [DoubleVectorValueStatistics] and returns it.
     *
     * @return Copy of this [DoubleVectorValueStatistics].
     */
    override fun copy(): DoubleVectorValueStatistics {
        val copy = DoubleVectorValueStatistics(this.type.logicalSize)
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