package org.vitrivr.cottontail.dbms.statistics.values

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.DateValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream
import java.lang.Long.max
import java.lang.Long.min

/**
 * A [ValueStatistics] implementation for [DateValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class DateValueStatistics : AbstractValueStatistics<DateValue>(Types.Date) {

    /**
     * Xodus serializer for [DateValueStatistics]
     */
    object Binding: XodusBinding<DateValueStatistics> {
        override fun read(stream: ByteArrayInputStream): DateValueStatistics {
            val stat = DateValueStatistics()
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.min = DateValue(LongBinding.readCompressed(stream))
            stat.max = DateValue(LongBinding.readCompressed(stream))
            return stat
        }

        override fun write(output: LightOutputStream, statistics: DateValueStatistics) {
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.writeCompressed(output, statistics.min.value)
            LongBinding.writeCompressed(output, statistics.max.value)
        }
    }

    /** Minimum value seen by this [DateValueStatistics]. */
    var min: DateValue = DateValue(Long.MAX_VALUE)
        private set

    /** Minimum value seen by this [DateValueStatistics]. */
    var max: DateValue = DateValue(Long.MIN_VALUE)
            private set
    /**
     * Updates this [DateValueStatistics] with an inserted [DateValue]
     *
     * @param inserted The [DateValue] that was inserted.
     */
    override fun insert(inserted: DateValue?) {
        super.insert(inserted)
        if (inserted != null) {
            this.min = DateValue(min(inserted.value, this.min.value))
            this.max = DateValue(max(inserted.value, this.max.value))
        }
    }

    /**
     * Updates this [DateValueStatistics] with a deleted [DateValue]
     *
     * @param deleted The [DateValue] that was deleted.
     */
    override fun delete(deleted: DateValue?) {
        super.delete(deleted)

        /* We cannot create a sensible estimate if a value is deleted. */
        if (this.min == deleted || this.max == deleted) {
            this.fresh = false
        }
    }

    /**
     * Resets this [DateValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = DateValue(Long.MAX_VALUE)
        this.max = DateValue(Long.MIN_VALUE)
    }

    /**
     * Copies this [DateValueStatistics] and returns it.
     *
     * @return Copy of this [DateValueStatistics].
     */
    override fun copy(): DateValueStatistics {
        val copy = DateValueStatistics()
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        copy.min = this.min
        copy.max = this.max
        return copy
    }
}