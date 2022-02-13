package org.vitrivr.cottontail.dbms.statistics.columns

import jetbrains.exodus.bindings.FloatBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.FloatValue
import org.vitrivr.cottontail.core.values.types.Types
import java.io.ByteArrayInputStream
import java.lang.Float.max
import java.lang.Float.min

/**
 * A [ValueStatistics] implementation for [FloatValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class FloatValueStatistics : ValueStatistics<FloatValue>(Types.Float) {

    /**
     * Xodus serializer for [FloatValueStatistics]
     */
    object Binding {
        fun read(stream: ByteArrayInputStream): FloatValueStatistics {
            val stat = FloatValueStatistics()
            stat.min = FloatBinding.BINDING.readObject(stream)
            stat.max = FloatBinding.BINDING.readObject(stream)
            stat.sum = FloatBinding.BINDING.readObject(stream)
            return stat
        }

        fun write(output: LightOutputStream, statistics: FloatValueStatistics) {
            FloatBinding.BINDING.writeObject(output, statistics.min)
            FloatBinding.BINDING.writeObject(output, statistics.max)
            FloatBinding.BINDING.writeObject(output, statistics.sum)
        }
    }

    /** Minimum value in this [FloatValueStatistics]. */
    var min: Float = Float.MAX_VALUE

    /** Minimum value in this [FloatValueStatistics]. */
    var max: Float = Float.MIN_VALUE

    /** Sum of all floats values in this [FloatValueStatistics]. */
    var sum: Float = 0.0f

    /** The arithmetic mean for the values seen by this [FloatValueStatistics]. */
    val mean: Float
        get() = (this.sum / this.numberOfNonNullEntries)

    /**
     * Updates this [FloatValueStatistics] with an inserted [FloatValue]
     *
     * @param inserted The [FloatValue] that was inserted.
     */
    override fun insert(inserted: FloatValue?) {
        super.insert(inserted)
        if (inserted != null) {
            this.min = min(inserted.value, this.min)
            this.max = max(inserted.value, this.max)
            this.sum += inserted.value
        }
    }

    /**
     * Updates this [FloatValueStatistics] with a deleted [FloatValue]
     *
     * @param deleted The [FloatValue] that was deleted.
     */
    override fun delete(deleted: FloatValue?) {
        super.delete(deleted)
        if (deleted != null) {
            this.sum -= deleted.value

            /* We cannot create a sensible estimate if a value is deleted. */
            if (this.min == deleted.value || this.max == deleted.value) {
                this.fresh = false
            }
        }
    }

    /**
     * Resets this [FloatValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = Float.MAX_VALUE
        this.max = Float.MIN_VALUE
        this.sum = 0.0f
    }

    /**
     * Copies this [FloatValueStatistics] and returns it.
     *
     * @return Copy of this [FloatValueStatistics].
     */
    override fun copy(): FloatValueStatistics {
        val copy = FloatValueStatistics()
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        copy.min = this.min
        copy.max = this.max
        copy.sum = this.sum
        return copy
    }
}