package org.vitrivr.cottontail.dbms.statistics.columns

import com.google.common.primitives.Shorts.max
import com.google.common.primitives.Shorts.min
import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.ShortBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.ShortValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueStatistics] implementation for [ShortValue]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class ShortValueStatistics : RealValueStatistics<ShortValue>(Types.Short) {

    /**
     * Xodus serializer for [ShortValueStatistics]
     */
    object Binding: XodusBinding<ShortValueStatistics> {
        override fun read(stream: ByteArrayInputStream): ShortValueStatistics {
            val stat = ShortValueStatistics()
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.min = ShortValue(ShortBinding.BINDING.readObject(stream))
            stat.max = ShortValue(ShortBinding.BINDING.readObject(stream))
            return stat
        }

        override fun write(output: LightOutputStream, statistics: ShortValueStatistics) {
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            ShortBinding.BINDING.writeObject(output, statistics.min.value)
            ShortBinding.BINDING.writeObject(output, statistics.max.value)
        }
    }

    /** Minimum value seen by this [ShortValueStatistics]. */
    override var min: ShortValue = ShortValue.MAX_VALUE
        private set

    /** Minimum value seen by this [ShortValueStatistics]. */
    override var max: ShortValue = ShortValue.MIN_VALUE
        private set

    /** Sum of all [IntValue]s seen by this [ShortValueStatistics]. */
    override var sum: DoubleValue = DoubleValue.ZERO
        private set

    /**
     * Updates this [ShortValueStatistics] with an inserted [ShortValue]
     *
     * @param inserted The [ShortValue] that was inserted.
     */
    override fun insert(inserted: ShortValue?) {
        super.insert(inserted)
        if (inserted != null) {
            this.min = ShortValue(min(inserted.value, this.min.value))
            this.max = ShortValue(max(inserted.value, this.max.value))
        }
    }

    /**
     * Updates this [ShortValueStatistics] with a deleted [ShortValue]
     *
     * @param deleted The [ShortValue] that was deleted.
     */
    override fun delete(deleted: ShortValue?) {
        super.delete(deleted)

        /* We cannot create a sensible estimate if a value is deleted. */
        if (this.min == deleted || this.max == deleted) {
            this.fresh = false
        }
    }

    /**
     * Resets this [ShortValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = ShortValue.MAX_VALUE
        this.max = ShortValue.MIN_VALUE
        this.sum = DoubleValue.ZERO
    }

    /**
     * Copies this [ShortValueStatistics] and returns it.
     *
     * @return Copy of this [ShortValueStatistics].
     */
    override fun copy(): ShortValueStatistics {
        val copy = ShortValueStatistics()
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        copy.min = this.min
        copy.max = this.max
        return copy
    }
}