package org.vitrivr.cottontail.dbms.statistics.columns

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.BooleanVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import java.io.ByteArrayInputStream

/**
 * A [ValueStatistics] implementation for [BooleanVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class BooleanVectorValueStatistics(type: Types<BooleanVectorValue>) : ValueStatistics<BooleanVectorValue>(type) {

    /**
     * Xodus serializer for [BooleanVectorValueStatistics]
     */
    object Binding {
        fun read(stream: ByteArrayInputStream, type: Types<BooleanVectorValue>): BooleanVectorValueStatistics {
            val stat = BooleanVectorValueStatistics(type)
            for (i in 0 until type.logicalSize) {
                stat.numberOfTrueEntries[i] = LongBinding.readCompressed(stream)
                stat.numberOfFalseEntries[i] = LongBinding.readCompressed(stream)
            }
            return stat
        }

        fun write(output: LightOutputStream, statistics: BooleanVectorValueStatistics) {
            for (i in 0 until statistics.type.logicalSize) {
                LongBinding.writeCompressed(output, statistics.numberOfTrueEntries[i])
                LongBinding.writeCompressed(output, statistics.numberOfFalseEntries[i])
            }
        }
    }

    /** A histogram capturing the number of true entries per component. */
    val numberOfTrueEntries: LongArray = LongArray(this.type.logicalSize)

    /** A histogram capturing the number of false entries per component. */
    val numberOfFalseEntries: LongArray
        get() = LongArray(this.type.logicalSize) {
            this.numberOfNonNullEntries - this.numberOfTrueEntries[it]
        }

    /**
     * Updates this [DoubleVectorValueStatistics] with an inserted [DoubleVectorValue]
     *
     * @param inserted The [Value] that was deleted.
     */
    override fun insert(inserted: BooleanVectorValue?) {
        super.insert(inserted)
        if (inserted != null) {
            for ((i, d) in inserted.data.withIndex()) {
                if (d) this.numberOfTrueEntries[i] = this.numberOfTrueEntries[i] + 1
            }
        }
    }

    /**
     * Updates this [DoubleVectorValueStatistics] with a deleted [DoubleVectorValue]
     *
     * @param deleted The [Value] that was deleted.
     */
    override fun delete(deleted: BooleanVectorValue?) {
        super.delete(deleted)
        if (deleted != null) {
            for ((i, d) in deleted.data.withIndex()) {
                if (d) this.numberOfTrueEntries[i] = this.numberOfTrueEntries[i] - 1
            }
        }
    }

    /**
     * Resets this [BooleanVectorValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        for (i in 0 until this.type.logicalSize) {
            this.numberOfTrueEntries[i] = 0L
        }
    }

    /**
     * Copies this [BooleanVectorValueStatistics] and returns it.
     *
     * @return Copy of this [BooleanVectorValueStatistics].
     */
    override fun copy(): BooleanVectorValueStatistics {
        val copy = BooleanVectorValueStatistics(this.type)
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        for (i in 0 until this.type.logicalSize) {
            copy.numberOfTrueEntries[i] = this.numberOfTrueEntries[i]
        }
        return copy
    }
}