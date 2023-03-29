package org.vitrivr.cottontail.dbms.statistics.values

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.BooleanVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueStatistics] implementation for [BooleanVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class BooleanVectorValueStatistics(logicalSize: Int) : AbstractValueStatistics<BooleanVectorValue>(Types.BooleanVector(logicalSize)) {

    /**
     * Xodus serializer for [BooleanVectorValueStatistics]
     */
    class Binding(val logicalSize: Int): XodusBinding<BooleanVectorValueStatistics> {
        override fun read(stream: ByteArrayInputStream): BooleanVectorValueStatistics {
            val stat = BooleanVectorValueStatistics(this.logicalSize)
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            for (i in 0 until this.logicalSize) {
                stat.numberOfTrueEntries[i] = LongBinding.readCompressed(stream)
                stat.numberOfFalseEntries[i] = LongBinding.readCompressed(stream)
            }
            return stat
        }

        override fun write(output: LightOutputStream, statistics: BooleanVectorValueStatistics) {
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
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
     * Updates this [BooleanValueStatistics] with an inserted [BooleanVectorValue]
     *
     * @param inserted The [BooleanVectorValue] that was deleted.
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
     * Updates this [BooleanValueStatistics] with a deleted [BooleanVectorValue]
     *
     * @param deleted The [BooleanVectorValue] that was deleted.
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
        val copy = BooleanVectorValueStatistics(this.type.logicalSize)
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        for (i in 0 until this.type.logicalSize) {
            copy.numberOfTrueEntries[i] = this.numberOfTrueEntries[i]
        }
        return copy
    }
}