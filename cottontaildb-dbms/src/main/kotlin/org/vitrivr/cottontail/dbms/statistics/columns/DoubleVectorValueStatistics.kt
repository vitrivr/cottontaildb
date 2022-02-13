package org.vitrivr.cottontail.dbms.statistics.columns

import jetbrains.exodus.bindings.DoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import java.io.ByteArrayInputStream

/**
 * A [ValueStatistics] implementation for [DoubleVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class DoubleVectorValueStatistics(type: Types<DoubleVectorValue>) : ValueStatistics<DoubleVectorValue>(type) {
    /** Minimum value in this [DoubleVectorValueStatistics]. */
    val min: DoubleVectorValue = DoubleVectorValue(DoubleArray(this.type.logicalSize) { Double.MAX_VALUE })

    /** Minimum value in this [DoubleVectorValueStatistics]. */
    val max: DoubleVectorValue = DoubleVectorValue(DoubleArray(this.type.logicalSize) { Double.MIN_VALUE })

    /** Sum of all floats values in this [DoubleVectorValueStatistics]. */
    val sum: DoubleVectorValue = DoubleVectorValue(DoubleArray(this.type.logicalSize))

    /** The arithmetic for the values seen by this [DoubleVectorValueStatistics]. */
    val avg: DoubleVectorValue
        get() = DoubleVectorValue(DoubleArray(this.type.logicalSize) {
            this.sum[it].value / this.numberOfNonNullEntries
        })

    /**
     * Xodus serializer for [DoubleVectorValueStatistics]
     */
    object Binding {
        fun read(stream: ByteArrayInputStream, type: Types<DoubleVectorValue>): DoubleVectorValueStatistics {
            val stat = DoubleVectorValueStatistics(type)
            for (i in 0 until type.logicalSize) {
                stat.min.data[i] = DoubleBinding.BINDING.readObject(stream)
                stat.max.data[i] = DoubleBinding.BINDING.readObject(stream)
                stat.sum.data[i] = DoubleBinding.BINDING.readObject(stream)
            }
            return stat
        }

        fun write(output: LightOutputStream, statistics: DoubleVectorValueStatistics) {
            for (i in 0 until statistics.type.logicalSize) {
                DoubleBinding.BINDING.writeObject(output, statistics.min.data[i])
                DoubleBinding.BINDING.writeObject(output, statistics.max.data[i])
                DoubleBinding.BINDING.writeObject(output, statistics.sum.data[i])
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
        val copy = DoubleVectorValueStatistics(this.type)
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