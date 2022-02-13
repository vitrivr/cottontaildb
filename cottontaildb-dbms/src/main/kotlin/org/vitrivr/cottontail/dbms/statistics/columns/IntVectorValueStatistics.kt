package org.vitrivr.cottontail.dbms.statistics.columns

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.IntVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import java.io.ByteArrayInputStream
import java.lang.Integer.max
import java.lang.Integer.min

/**
 * A [ValueStatistics] implementation for [IntVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class IntVectorValueStatistics(type: Types<IntVectorValue>) : ValueStatistics<IntVectorValue>(type) {
    /** Minimum value in this [IntVectorValueStatistics]. */
    val min: IntVectorValue = IntVectorValue(IntArray(this.type.logicalSize) { Int.MAX_VALUE })

    /** Minimum value in this [IntVectorValueStatistics]. */
    val max: IntVectorValue = IntVectorValue(IntArray(this.type.logicalSize) { Int.MIN_VALUE })

    /**
     * Xodus serializer for [IntVectorValueStatistics]
     */
    object Binding {
        fun read(stream: ByteArrayInputStream, type: Types<IntVectorValue>): IntVectorValueStatistics {
            val stat = IntVectorValueStatistics(type)
            for (i in 0 until type.logicalSize) {
                stat.min.data[i] = IntegerBinding.BINDING.readObject(stream)
                stat.max.data[i] = IntegerBinding.BINDING.readObject(stream)
            }
            return stat
        }

        fun write(output: LightOutputStream, statistics: IntVectorValueStatistics) {
            for (i in 0 until statistics.type.logicalSize) {
                IntegerBinding.BINDING.writeObject(output, statistics.min.data[i])
                IntegerBinding.BINDING.writeObject(output, statistics.max.data[i])
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
        val copy = IntVectorValueStatistics(this.type)
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        for (i in 0 until this.type.logicalSize) {
            copy.min.data[i] = this.min.data[i]
            copy.max.data[i] = this.max.data[i]
        }
        return copy
    }
}