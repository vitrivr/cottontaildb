package org.vitrivr.cottontail.dbms.statistics.columns

import com.google.common.primitives.SignedBytes.max
import com.google.common.primitives.SignedBytes.min
import jetbrains.exodus.bindings.ByteBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.ByteValue
import org.vitrivr.cottontail.core.values.types.Types
import java.io.ByteArrayInputStream

/**
 * A [ValueStatistics] implementation for [ByteValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class ByteValueStatistics : ValueStatistics<ByteValue>(Types.Byte) {

    /**
     * Xodus serializer for [ByteValueStatistics]
     */
    object Binding {
        fun read(stream: ByteArrayInputStream): ByteValueStatistics {
            val stat = ByteValueStatistics()
            stat.min = ByteBinding.BINDING.readObject(stream)
            stat.max = ByteBinding.BINDING.readObject(stream)
            return stat
        }

        fun write(output: LightOutputStream, statistics: ByteValueStatistics) {
            ByteBinding.BINDING.writeObject(output, statistics.min)
            ByteBinding.BINDING.writeObject(output, statistics.max)
        }
    }

    /** Minimum value for this [ByteValueStatistics]. */
    var min: Byte = Byte.MAX_VALUE

    /** Minimum value for this [ByteValueStatistics]. */
    var max: Byte = Byte.MIN_VALUE

    /**
     * Updates this [ByteValueStatistics] with an inserted [ByteValue]
     *
     * @param inserted The [ByteValue] that was inserted.
     */
    override fun insert(inserted: ByteValue?) {
        super.insert(inserted)
        if (inserted != null) {
            this.min = min(inserted.value, this.min)
            this.max = max(inserted.value, this.max)
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
        if (deleted != null) {
            if (this.min == deleted.value || this.max == deleted.value) {
                this.fresh = false
            }
        }
    }

    /**
     * Resets this [ByteValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = Byte.MAX_VALUE
        this.max = Byte.MIN_VALUE
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