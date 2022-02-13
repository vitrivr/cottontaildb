package org.vitrivr.cottontail.dbms.statistics.columns

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.types.Types
import java.io.ByteArrayInputStream
import java.lang.Long.max
import java.lang.Long.min

/**
 * A [ValueStatistics] implementation for [LongValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class LongValueStatistics : ValueStatistics<LongValue>(Types.Long) {

    /**
     * Xodus serializer for [LongValueStatistics]
     */
    object Binding {
        fun read(stream: ByteArrayInputStream): LongValueStatistics {
            val stat = LongValueStatistics()
            stat.min = LongBinding.BINDING.readObject(stream)
            stat.max = LongBinding.BINDING.readObject(stream)
            return stat
        }

        fun write(output: LightOutputStream, statistics: LongValueStatistics) {
            LongBinding.BINDING.writeObject(output, statistics.min)
            LongBinding.BINDING.writeObject(output, statistics.max)
        }
    }

    /** Minimum value for this [LongValueStatistics]. */
    var min: Long = Long.MAX_VALUE

    /** Minimum value for this [LongValueStatistics]. */
    var max: Long = Long.MIN_VALUE

    /**
     * Updates this [LongValueStatistics] with an inserted [LongValue]
     *
     * @param inserted The [LongValue] that was inserted.
     */
    override fun insert(inserted: LongValue?) {
        super.insert(inserted)
        if (inserted != null) {
            this.min = min(inserted.value, this.min)
            this.max = max(inserted.value, this.max)
        }
    }

    /**
     * Updates this [LongValueStatistics] with a deleted [LongValue]
     *
     * @param deleted The [LongValue] that was deleted.
     */
    override fun delete(deleted: LongValue?) {
        super.delete(deleted)

        /* We cannot create a sensible estimate if a value is deleted. */
        if (this.min == deleted?.value || this.max == deleted?.value) {
            this.fresh = false
        }
    }

    /**
     * Resets this [LongValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = Long.MAX_VALUE
        this.max = Long.MIN_VALUE
    }

    /**
     * Copies this [LongValueStatistics] and returns it.
     *
     * @return Copy of this [LongValueStatistics].
     */
    override fun copy(): LongValueStatistics {
        val copy = LongValueStatistics()
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        copy.min = this.min
        copy.max = this.max
        return copy
    }
}