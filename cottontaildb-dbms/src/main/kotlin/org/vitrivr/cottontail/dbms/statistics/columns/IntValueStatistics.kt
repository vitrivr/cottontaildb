package org.vitrivr.cottontail.dbms.statistics.columns

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.types.Types
import java.io.ByteArrayInputStream
import java.lang.Integer.max
import java.lang.Integer.min

/**
 * A [ValueStatistics] implementation for [IntValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class IntValueStatistics : ValueStatistics<IntValue>(Types.Int) {

    /**
     * Xodus serializer for [IntValueStatistics]
     */
    object Binding {
        fun read(stream: ByteArrayInputStream): IntValueStatistics {
            val stat = IntValueStatistics()
            stat.min = IntegerBinding.BINDING.readObject(stream)
            stat.max = IntegerBinding.BINDING.readObject(stream)
            return stat
        }

        fun write(output: LightOutputStream, statistics: IntValueStatistics) {
            IntegerBinding.BINDING.writeObject(output, statistics.min)
            IntegerBinding.BINDING.writeObject(output, statistics.max)
        }
    }

    /** Minimum value for this [IntValueStatistics]. */
    var min: Int = Int.MAX_VALUE

    /** Minimum value for this [IntValueStatistics]. */
    var max: Int = Int.MIN_VALUE

    /**
     * Updates this [IntValueStatistics] with an inserted [IntValue]
     *
     * @param inserted The [IntValue] that was inserted.
     */
    override fun insert(inserted: IntValue?) {
        super.insert(inserted)
        if (inserted != null) {
            this.min = min(inserted.value, this.min)
            this.max = max(inserted.value, this.max)
        }
    }

    /**
     * Updates this [IntValueStatistics] with a deleted [IntValue]
     *
     * @param deleted The [IntValue] that was deleted.
     */
    override fun delete(deleted: IntValue?) {
        super.delete(deleted)

        /* We cannot create a sensible estimate if a value is deleted. */
        if (this.min == deleted?.value || this.max == deleted?.value) {
            this.fresh = false
        }
    }

    /**
     * Resets this [IntValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = Int.MAX_VALUE
        this.max = Int.MIN_VALUE
    }

    /**
     * Copies this [IntValueStatistics] and returns it.
     *
     * @return Copy of this [IntValueStatistics].
     */
    override fun copy(): IntValueStatistics {
        val copy = IntValueStatistics()
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        copy.min = this.min
        copy.max = this.max
        return copy
    }
}