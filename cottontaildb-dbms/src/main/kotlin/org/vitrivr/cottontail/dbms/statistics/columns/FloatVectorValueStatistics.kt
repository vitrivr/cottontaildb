package org.vitrivr.cottontail.dbms.statistics.columns

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedFloatBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream
import java.lang.Float.max
import java.lang.Float.min

/**
 * A [ValueStatistics] implementation for [FloatVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class FloatVectorValueStatistics(logicalSize: Int) : AbstractValueStatistics<FloatVectorValue>(Types.FloatVector(logicalSize)), VectorValueStatistics<FloatVectorValue> {
    /** Minimum value in this [FloatVectorValueStatistics]. */
    override val min: FloatVectorValue = FloatVectorValue(FloatArray(this.type.logicalSize) { Float.MAX_VALUE })

    /** Minimum value in this [FloatVectorValueStatistics]. */
    override val max: FloatVectorValue = FloatVectorValue(FloatArray(this.type.logicalSize) { Float.MIN_VALUE })

    /** Sum of all floats values in this [FloatVectorValueStatistics]. */
    override val sum: FloatVectorValue = FloatVectorValue(FloatArray(this.type.logicalSize))

    /** The arithmetic for the values seen by this [DoubleVectorValueStatistics]. */
    override val mean: FloatVectorValue
        get() = FloatVectorValue(FloatArray(this.type.logicalSize) {
            this.sum[it].value / this.numberOfNonNullEntries
        })

    /**
     * Xodus serializer for [FloatVectorValueStatistics]
     */
    class Binding(val logicalSize: Int): XodusBinding<FloatVectorValueStatistics> {
        override fun read(stream: ByteArrayInputStream): FloatVectorValueStatistics {
            val stat = FloatVectorValueStatistics(logicalSize)
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            for (i in 0 until this.logicalSize) {
                stat.min.data[i] = SignedFloatBinding.BINDING.readObject(stream)
                stat.max.data[i] = SignedFloatBinding.BINDING.readObject(stream)
                stat.sum.data[i] = SignedFloatBinding.BINDING.readObject(stream)
            }
            return stat
        }

        override fun write(output: LightOutputStream, statistics: FloatVectorValueStatistics) {
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            for (i in 0 until statistics.type.logicalSize) {
                SignedFloatBinding.BINDING.writeObject(output, statistics.min.data[i])
                SignedFloatBinding.BINDING.writeObject(output, statistics.max.data[i])
                SignedFloatBinding.BINDING.writeObject(output, statistics.sum.data[i])
            }
        }
    }

    /**
     * Updates this [FloatVectorValueStatistics] with an inserted [FloatVectorValue]
     *
     * @param inserted The [FloatVectorValue] that was inserted.
     */
    override fun insert(inserted: FloatVectorValue?) {
        super.insert(inserted)
        if (inserted != null) {
            for ((i, d) in inserted.data.withIndex()) {
                this.min.data[i] = min(d, this.min.data[i])
                this.max.data[i] = max(d, this.max.data[i])
                this.sum.data[i] += d
            }
        }
    }

    /**
     * Updates this [FloatVectorValueStatistics] with a deleted [FloatVectorValue]
     *
     * @param deleted The [FloatVectorValue] that was deleted.
     */
    override fun delete(deleted: FloatVectorValue?) {
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
     * Resets this [FloatVectorValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        for (i in 0 until this.type.logicalSize) {
            this.min.data[i] = Float.MAX_VALUE
            this.max.data[i] = Float.MIN_VALUE
            this.sum.data[i] = 0.0f
        }
    }

    /**
     * Copies this [FloatVectorValueStatistics] and returns it.
     *
     * @return Copy of this [FloatVectorValueStatistics].
     */
    override fun copy(): FloatVectorValueStatistics {
        val copy = FloatVectorValueStatistics(this.type.logicalSize)
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