package org.vitrivr.cottontail.dbms.statistics.values

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream
import java.lang.Integer.max
import java.lang.Integer.min

/**
 * A [ValueStatistics] implementation for [IntValue]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class IntValueStatistics : RealValueStatistics<IntValue>(Types.Int) {

    /**
     * Xodus serializer for [IntValueStatistics]
     */
    object Binding: XodusBinding<IntValueStatistics> {
        override fun read(stream: ByteArrayInputStream): IntValueStatistics {
            val stat = IntValueStatistics()
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.min = IntValue(IntegerBinding.BINDING.readObject(stream))
            stat.max = IntValue(IntegerBinding.BINDING.readObject(stream))
            stat.sum = DoubleValue(SignedDoubleBinding.BINDING.readObject(stream))
            return stat
        }

        override fun write(output: LightOutputStream, statistics: IntValueStatistics) {
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            IntegerBinding.BINDING.writeObject(output, statistics.min.value)
            IntegerBinding.BINDING.writeObject(output, statistics.max.value)
            SignedDoubleBinding.BINDING.writeObject(output, statistics.sum.value)
        }
    }

    /** Minimum value seen by this [IntValueStatistics]. */
    override var min: IntValue = IntValue.MAX_VALUE
        private set

    /** Minimum value seen by this [IntValueStatistics]. */
    override var max: IntValue = IntValue.MIN_VALUE
        private set

    /** Sum of all [IntValue]s seen by this [IntValueStatistics]. */
    override var sum: DoubleValue = DoubleValue.ZERO
        private set

    /**
     * Updates this [IntValueStatistics] with an inserted [IntValue]
     *
     * @param inserted The [IntValue] that was inserted.
     */
    override fun insert(inserted: IntValue?) {
        super.insert(inserted)
        if (inserted != null) {
            this.min = IntValue(min(inserted.value, this.min.value))
            this.max = IntValue(max(inserted.value, this.max.value))
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
        if (this.min == deleted || this.max == deleted) {
            this.fresh = false
        }
    }

    /**
     * Resets this [IntValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = IntValue.MAX_VALUE
        this.max = IntValue.MIN_VALUE
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