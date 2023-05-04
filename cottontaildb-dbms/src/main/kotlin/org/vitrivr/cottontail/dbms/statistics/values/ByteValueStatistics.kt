package org.vitrivr.cottontail.dbms.statistics.values

import com.google.common.primitives.SignedBytes.max
import com.google.common.primitives.SignedBytes.min
import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.ByteBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ByteValue
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueStatistics] implementation for [ByteValue]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class ByteValueStatistics : RealValueStatistics<ByteValue>(Types.Byte) {

    /**
     * Xodus serializer for [ByteValueStatistics]
     */
    object Binding: XodusBinding<ByteValueStatistics> {
        override fun read(stream: ByteArrayInputStream): ByteValueStatistics {
            val stat = ByteValueStatistics()
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.min = ByteValue(ByteBinding.BINDING.readObject(stream))
            stat.max = ByteValue(ByteBinding.BINDING.readObject(stream))
            return stat
        }

        override fun write(output: LightOutputStream, statistics: ByteValueStatistics) {
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            ByteBinding.BINDING.writeObject(output, statistics.min.value)
            ByteBinding.BINDING.writeObject(output, statistics.max.value)
        }
    }

    /** Minimum value seen by this [ByteValueStatistics]. */
    override var min: ByteValue = ByteValue.MAX_VALUE
        private set

    /** Minimum value seen by this [ByteValueStatistics]. */
    override var max: ByteValue = ByteValue.MIN_VALUE
        private set

    /** Sum of all [ByteValue]s seen by this [ByteValueStatistics]. */
    override var sum: DoubleValue = DoubleValue.ZERO
        private set

    /**
     * Updates this [ByteValueStatistics] with an inserted [ByteValue]
     *
     * @param inserted The [ByteValue] that was inserted.
     */
    override fun insert(inserted: ByteValue?) {
        super.insert(inserted)
        if (inserted != null) {
            this.min = ByteValue(min(inserted.value, this.min.value))
            this.max = ByteValue(max(inserted.value, this.max.value))
            this.sum += DoubleValue(inserted.value)
        }
    }

    /**
     * Updates this [ByteValueStatistics] with a deleted [ByteValue]
     *
     * @param deleted The [ByteValue] that was deleted.
     */
    override fun delete(deleted: ByteValue?) {
        super.delete(deleted)

        /* We cannot create a sensible estimate if a value is deleted. */
        if (this.min == deleted || this.max == deleted) {
            this.fresh = false
        }
    }

    /**
     * Resets this [ByteValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = ByteValue.MAX_VALUE
        this.max = ByteValue.MIN_VALUE
        this.sum = DoubleValue.ZERO
    }

    /**
     * Copies this [ByteValueStatistics] and returns it.
     *
     * @return Copy of this [ByteValueStatistics].
     */
    override fun copy(): ByteValueStatistics {
        val copy = ByteValueStatistics()
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        copy.min = this.min
        copy.max = this.max
        return copy
    }
}