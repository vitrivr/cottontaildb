package org.vitrivr.cottontail.dbms.statistics.values

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream
import java.lang.Double.max
import java.lang.Double.min

/**
 * A [RealValueStatistics] implementation for [DoubleValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class DoubleValueStatistics : RealValueStatistics<DoubleValue>(Types.Double) {

    /**
     * Xodus serializer for [DoubleValueStatistics]
     */
    object Binding: XodusBinding<DoubleValueStatistics> {
        override fun read(stream: ByteArrayInputStream): DoubleValueStatistics {
            val stat = DoubleValueStatistics()
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.min = DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
            stat.max = DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
            stat.sum = DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
            return stat
        }

        override fun write(output: LightOutputStream, statistics: DoubleValueStatistics) {
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            SignedDoubleBinding.BINDING.writeObject(output, statistics.min.value)
            SignedDoubleBinding.BINDING.writeObject(output, statistics.max.value)
            SignedDoubleBinding.BINDING.writeObject(output, statistics.sum.value)
        }
    }

    /** Minimum value in this [DoubleValueStatistics]. */
    override var min: DoubleValue = DoubleValue.MAX_VALUE
        private set

    /** Minimum value in this [DoubleValueStatistics]. */
    override var max: DoubleValue = DoubleValue.MAX_VALUE
        private set

    /** Sum of all floats values in this [DoubleValueStatistics]. */
    override var sum: DoubleValue = DoubleValue.ZERO
        private set

    /**
     * Updates this [DoubleValueStatistics] with an inserted [DoubleValue]
     *
     * @param inserted The [DoubleValue] that was inserted.
     */
    override fun insert(inserted: DoubleValue?) {
        super.insert(inserted)
        if (inserted != null) {
            this.min = DoubleValue(min(inserted.value, this.min.value))
            this.max = DoubleValue(max(inserted.value, this.max.value))
            this.sum += DoubleValue(inserted.value)
        }
    }

    /**
     * Updates this [DoubleValueStatistics] with a deleted [DoubleValue]
     *
     * @param deleted The [DoubleValue] that was deleted.
     */
    override fun delete(deleted: DoubleValue?) {
        super.delete(deleted)
        if (deleted != null) {
            this.sum -= deleted
            if (this.min == deleted || this.max == deleted) {
                this.fresh = false
            }
        }
    }

    /**
     * Resets this [DoubleValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = DoubleValue.MAX_VALUE
        this.max = DoubleValue.MIN_VALUE
        this.sum = DoubleValue.ZERO
    }

    /**
     * Copies this [DoubleValueStatistics] and returns it.
     *
     * @return Copy of this [DoubleValueStatistics].
     */
    override fun copy(): DoubleValueStatistics {
        val copy = DoubleValueStatistics()
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        copy.min = this.min
        copy.max = this.max
        copy.sum = this.sum
        return copy
    }
}