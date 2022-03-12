package org.vitrivr.cottontail.dbms.statistics.columns

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream
import java.lang.Long.max
import java.lang.Long.min

/**
 * A [ValueStatistics] implementation for [LongValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class LongValueStatistics : AbstractValueStatistics<LongValue>(Types.Long), RealValueStatistics<LongValue> {

    /**
     * Xodus serializer for [LongValueStatistics]
     */
    object Binding: XodusBinding<LongValueStatistics> {
        override fun read(stream: ByteArrayInputStream): LongValueStatistics {
            val stat = LongValueStatistics()
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.min = LongValue(LongBinding.BINDING.readObject(stream))
            stat.max = LongValue(LongBinding.BINDING.readObject(stream))
            return stat
        }

        override fun write(output: LightOutputStream, statistics: LongValueStatistics) {
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.BINDING.writeObject(output, statistics.min)
            LongBinding.BINDING.writeObject(output, statistics.max)
        }
    }

    /** Minimum value seen by this [LongValueStatistics]. */
    override var min: LongValue = LongValue.MAX_VALUE
        private set

    /** Minimum value seen by this [LongValueStatistics]. */
    override var max: LongValue = LongValue.MIN_VALUE
        private set

    /** Sum of all [LongValue]s seen by this [LongValueStatistics]. */
    override var sum: DoubleValue = DoubleValue.ZERO
        private set

    /**
     * Updates this [LongValueStatistics] with an inserted [LongValue]
     *
     * @param inserted The [LongValue] that was inserted.
     */
    override fun insert(inserted: LongValue?) {
        super.insert(inserted)
        if (inserted != null) {
            this.min = LongValue(min(inserted.value, this.min.value))
            this.max = LongValue(max(inserted.value, this.max.value))
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
        if (this.min == deleted || this.max == deleted) {
            this.fresh = false
        }
    }

    /**
     * Resets this [LongValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = LongValue.MAX_VALUE
        this.max = LongValue.MIN_VALUE
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