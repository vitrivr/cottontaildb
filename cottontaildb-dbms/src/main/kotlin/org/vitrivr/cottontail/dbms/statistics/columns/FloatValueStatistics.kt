package org.vitrivr.cottontail.dbms.statistics.columns

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.DoubleBinding
import jetbrains.exodus.bindings.FloatBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.FloatValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream
import java.lang.Float.max
import java.lang.Float.min

/**
 * A [ValueStatistics] implementation for [FloatValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class FloatValueStatistics : AbstractValueStatistics<FloatValue>(Types.Float), RealValueStatistics<FloatValue> {

    /**
     * Xodus serializer for [FloatValueStatistics]
     */
    object Binding: XodusBinding<FloatValueStatistics> {
        override fun read(stream: ByteArrayInputStream): FloatValueStatistics {
            val stat = FloatValueStatistics()
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.min = FloatValue(FloatBinding.BINDING.readObject(stream))
            stat.max = FloatValue(FloatBinding.BINDING.readObject(stream))
            stat.sum = DoubleValue(FloatBinding.BINDING.readObject(stream))
            return stat
        }

        override fun write(output: LightOutputStream, statistics: FloatValueStatistics) {
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            FloatBinding.BINDING.writeObject(output, statistics.min)
            FloatBinding.BINDING.writeObject(output, statistics.max)
            DoubleBinding.BINDING.writeObject(output, statistics.sum)
        }
    }

    /** Minimum value seen by this [FloatValueStatistics]. */
    override var min: FloatValue = FloatValue.MAX_VALUE
        private set

    /** Minimum value seen by this [FloatValueStatistics]. */
    override var max: FloatValue = FloatValue.MIN_VALUE
        private set

    /** Sum of all [FloatValue]s seen by this [FloatValueStatistics]. */
    override var sum: DoubleValue = DoubleValue.ZERO
        private set

    /**
     * Updates this [FloatValueStatistics] with an inserted [FloatValue]
     *
     * @param inserted The [FloatValue] that was inserted.
     */
    override fun insert(inserted: FloatValue?) {
        super.insert(inserted)
        if (inserted != null) {
            this.min = FloatValue(min(inserted.value, this.min.value))
            this.max = FloatValue(max(inserted.value, this.max.value))
            this.sum += DoubleValue(inserted.value)
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
            this.sum -= deleted

            /* We cannot create a sensible estimate if a value is deleted. */
            if (this.min == deleted || this.max == deleted) {
                this.fresh = false
            }
        }
    }

    /**
     * Resets this [FloatValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = FloatValue.MAX_VALUE
        this.max = FloatValue.MIN_VALUE
        this.sum = DoubleValue.ZERO
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